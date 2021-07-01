from abc import ABC, abstractmethod
from typing import List
import pickle

import pandas as pd
import numpy as np
import lightgbm as lgb
import torch
from pytorch_forecasting.metrics import QuantileLoss
from pytorch_lightning.loggers import TensorBoardLogger
from sklearn.metrics import f1_score, confusion_matrix, roc_curve, accuracy_score, roc_auc_score
import seaborn as sns
import matplotlib.pyplot as plt
import joblib
from sklearn.model_selection import GridSearchCV
from sklearn.preprocessing import LabelEncoder

from base.config import logger

from base.consts import TrainerConfig, PreProcessConfig
from util.preprocess_util import Processor
import pytorch_lightning as pl
from pytorch_lightning.callbacks import EarlyStopping, LearningRateMonitor
from pytorch_forecasting.models.temporal_fusion_transformer.tuning import optimize_hyperparameters
from pytorch_forecasting import TimeSeriesDataSet, TemporalFusionTransformer, Baseline


class CategoricalEncoder(object):

    def __init__(self, df: pd.DataFrame, categorical_columns: List[str]):
        self.df = df
        self.categorical_columns = categorical_columns
        self.label_encoder = {}

    def transform(self):
        for label in self.categorical_columns:
            print(label)
            if self.df[label].dtype == "object":
                self.df[label].fillna("unknown", inplace=True)
            elif self.df[label].dtype == "float64":
                self.df[label].fillna(0.0, inplace=True)
            le = LabelEncoder()
            try:
                le.fit(self.df[label])
            except Exception as e:
                print(e)
            self.df[label] = le.transform(self.df[label])
            self.label_encoder[label] = le
        return self.df


def make_model(df: pd.DataFrame = pd.DataFrame(), model_name: str = "lightgbm", target: str = "train"):
    """根据模型名字生成对应的模型

    Args:
        df: 包含所有数据的dataframe
        model_name: 使用哪个模型
        target: 目标

    """

    if not df.shape[0]:
        # df = pd.read_csv(PreProcessConfig.VOLUME_RESULT_PATH)
        df = pd.read_csv(PreProcessConfig.MINUTE_RESULT_PATH)

    model_name = "torch"
    trainers = {
        "lightgbm": LightgbmTrainer,
        "torch": TorchTrainer
    }

    trainer = trainers[model_name](df)
    if target == "train":
        return trainer.get_trained_model()
    elif target == "tune":
        trainer.tune()
    else:
        raise Exception("please choose right target")


class BaseTrainer(ABC):

    @abstractmethod
    def get_trained_model(self):
        pass


class TorchTrainer(BaseTrainer):
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def prepare(self):
        data = self.df.iloc[:, 2:]
        # use pct change to make a use full model
        data["Close"] = data["Close"].pct_change(-1) * 100
        data = data.iloc[:-10,:]
        # define dataset
        max_encoder_length = 24
        max_prediction_length = 6
        data_total_length = len(data)
        train_test_ratio = 0.8

        # preprocess
        data["group"] = "btc"
        data["Log_Close"] = np.log(data.Close + 1e-8)
        data["candle_begin_time"] = pd.to_datetime(data["candle_begin_time"])
        data["weekday"] = data.candle_begin_time.dt.weekday.astype(str).astype("category")
        data["hour"] = data.candle_begin_time.dt.hour.astype(str).astype("category")
        data["first_tag"] = data.first_tag.fillna("unknown").astype(str).astype('category')
        data['time_idx'] = (data.candle_begin_time - data.iloc[0, :].candle_begin_time).astype("timedelta64[m]").astype(int)
        data.drop_duplicates(keep='last', subset=["time_idx"], inplace=True)
        # preprocess twitter
        data.twitter_usd_number.fillna(0, inplace=True)
        data.long_index.fillna(0, inplace=True)
        data.short_index.fillna(0, inplace=True)
        data.gold_Close.fillna(method="pad", inplace=True)
        # preprocess jinse
        jinse_top_tag = data.first_tag.value_counts().head(10).index.tolist()
        data["first_tag"] = data.first_tag.apply(lambda x: x if x in jinse_top_tag else "unknown")

        training = TimeSeriesDataSet(
            data[:int(data_total_length * train_test_ratio)],
            time_idx="time_idx",
            target="Close",
            # weight="weight",
            group_ids=["group"],
            min_encoder_length=max_encoder_length // 2,  # keep encoder length long (as it is in the validation set)
            max_encoder_length=max_encoder_length,
            max_prediction_length=max_prediction_length,
            static_categoricals=[],
            static_reals=[],
            # TODO: add more
            time_varying_known_categoricals=[
                "weekday", "hour"
            ],
            # TODO: add more
            time_varying_known_reals=[
                "time_idx",
            ],
            # TODO: add more
            time_varying_unknown_categoricals=[
                "first_tag"
            ],
            # TODO: add more
            time_varying_unknown_reals=[
                "Close", "Log_Close", "Volume", "twitter_usd_number", "long_index", "short_index", "gold_Close"
            ]
        )
        self.training = training
        # create validation set (predict=True) which means to predict the last max_prediction_length points in time for each series
        validation = TimeSeriesDataSet.from_dataset(training, data, predict=True, stop_randomization=True)
        print(training, validation)
        # create dataloaders for model
        batch_size = 128  # set this between 32 to 128
        self.train_dataloader = training.to_dataloader(train=True, batch_size=batch_size, num_workers=0)
        self.val_dataloader = validation.to_dataloader(train=False, batch_size=batch_size, num_workers=0)

        # self.find_optimal_learning_rate()

        self.train_model()
        self.evaluate_preformance()

        # self.hyperparameter_tuning()

    def find_optimal_learning_rate(self):
        actuals = torch.cat([y for x, y in iter(self.val_dataloader)])
        baseline_predictions = Baseline().predict(self.val_dataloader)
        print((actuals - baseline_predictions).abs().mean().item())
        # configure network and trainer
        pl.seed_everything(42)
        trainer = pl.Trainer(
            gpus=0,
            # clipping gradients is a hyperparameter and important to prevent divergance
            # of the gradient for recurrent neural networks
            gradient_clip_val=0.1,
        )

        tft = TemporalFusionTransformer.from_dataset(
            self.training,
            # not meaningful for finding the learning rate but otherwise very important
            learning_rate=0.03,
            hidden_size=16,  # most important hyperparameter apart from learning rate
            # number of attention heads. Set to up to 4 for large datasets
            attention_head_size=1,
            dropout=0.1,  # between 0.1 and 0.3 are good values
            hidden_continuous_size=8,  # set to <= hidden_size
            output_size=7,  # 7 quantiles by default
            loss=QuantileLoss(),
            # reduce learning rate if no improvement in validation loss after x epochs
            reduce_on_plateau_patience=4,
        )
        print(f"Number of parameters in network: {tft.size() / 1e3:.1f}k")
        # find optimal learning rate
        res = trainer.tuner.lr_find(
            tft,
            train_dataloader=self.train_dataloader,
            val_dataloaders=self.val_dataloader,
            max_lr=10.0,
            min_lr=1e-6,
        )

        print(f"suggested learning rate: {res.suggestion()}")
        fig = res.plot(show=True, suggest=True)
        fig.show()

    def train_model(self):
        # configure network and trainer
        early_stop_callback = EarlyStopping(monitor="val_loss", min_delta=1e-4, patience=10, verbose=False, mode="min")
        lr_logger = LearningRateMonitor()  # log the learning rate
        logger = TensorBoardLogger("lightning_logs")  # logging results to a tensorboard

        self.trainer = pl.Trainer(
            max_epochs=30,
            gpus=0,
            weights_summary="top",
            gradient_clip_val=0.1,
            limit_train_batches=30,  # coment in for training, running valiation every 30 batches
            # fast_dev_run=True,  # comment in to check that networkor dataset has no serious bugs
            callbacks=[lr_logger, early_stop_callback],
            logger=logger,
        )

        tft = TemporalFusionTransformer.from_dataset(
            self.training,
            learning_rate=0.03,
            hidden_size=16,
            attention_head_size=1,
            dropout=0.1,
            hidden_continuous_size=8,
            output_size=7,  # 7 quantiles by default
            loss=QuantileLoss(),
            log_interval=10,  # uncomment for learning rate finder and otherwise, e.g. to 10 for logging every 10 batches
            reduce_on_plateau_patience=4,
        )
        print(f"Number of parameters in network: {tft.size() / 1e3:.1f}k")

        self.trainer.fit(
            tft,
            train_dataloader=self.train_dataloader,
            val_dataloaders=self.val_dataloader,
        )

    def evaluate_preformance(self):
        best_model_path = self.trainer.checkpoint_callback.best_model_path
        best_tft = TemporalFusionTransformer.load_from_checkpoint(best_model_path)
        actuals = torch.cat([y for x, y in iter(self.val_dataloader)])
        predictions = best_tft.predict(self.val_dataloader)

        print(f"best_score:{(actuals - predictions).abs().mean()}")
        raw_predictions, x = best_tft.predict(self.val_dataloader, mode="raw", return_x=True)
        # best_tft.plot_prediction(x, raw_predictions, idx=0, add_loss_to_title=True)
        interpretation = best_tft.interpret_output(
            raw_predictions, reduction="sum"
        )
        best_tft.plot_interpretation(interpretation)

    def hyperparameter_tuning(self):
        # create study
        study = optimize_hyperparameters(
            self.train_dataloader,
            self.val_dataloader,
            model_path="optuna_test",
            n_trials=200,
            max_epochs=50,
            gradient_clip_val_range=(0.01, 1.0),
            hidden_size_range=(8, 128),
            hidden_continuous_size_range=(8, 128),
            attention_head_size_range=(1, 4),
            learning_rate_range=(0.001, 0.1),
            dropout_range=(0.1, 0.3),
            trainer_kwargs=dict(limit_train_batches=30),
            reduce_on_plateau_patience=4,
            use_learning_rate_finder=False,  # use Optuna to find ideal learning rate or use in-built learning rate finder
        )

        # save study results - also we can resume tuning at a later point in time
        with open("test_study.pkl", "wb") as fout:
            pickle.dump(study, fout)

        # show best hyperparameters
        print(study.best_trial.params)

    def get_trained_model(self):
        self.prepare()


class LightgbmTrainer(BaseTrainer):

    def __init__(self, df: pd.DataFrame):
        logger.info(f"feature columns: {TrainerConfig.feature_columns}")

        logger.info("start training...")
        self.categorical_encoder = CategoricalEncoder(df, TrainerConfig.categorical_columns)
        self.parameters = TrainerConfig.LightgbmParams.to_dict()
        # some feature is added just before train for test TODO: move it into preprocess part
        self.feature_columns = TrainerConfig.feature_columns
        self.model = lgb.LGBMClassifier(**self.parameters)
        print(self.model)
        self.df = self.preprocess(df)
        self.train_df, self.test_df = self.split_into_train_test()
        self.X_train, self.y_train = self.get_x_y(self.train_df)
        self.X_test, self.y_test = self.get_x_y(self.test_df)
        scale_pos_weights = self.y_test[~self.y_test].shape[0] / self.y_test[self.y_test].shape[0]
        print(scale_pos_weights)
        self.parameters.update({
            "scale_pos_weights": scale_pos_weights
        })

    def split_into_train_test(self, percent: float = 0.8):
        n = len(self.df)
        train_df = self.df[0:int(n * percent)]
        test_df = self.df[int(n * percent):]
        logger.info(f"Train data ratio: {train_df[train_df[TrainerConfig.label_column]].shape[0] / train_df.shape[0]}")
        logger.info(f"Test data ratio: {test_df[test_df[TrainerConfig.label_column]].shape[0] / test_df.shape[0]}")
        return train_df, test_df

    def get_x_y(self, df: pd.DataFrame):
        return df[self.feature_columns], df[TrainerConfig.label_column]

    def find_right_n_estimators(self):
        best_n_estimators = []
        for i in range(20):
            params = {
                'boosting_type': 'gbdt',
                'objective': 'binary',

                'learning_rate': 0.01,
                'num_leaves': 50,
                'max_depth': 6,

                'subsample': 0.8,
                'colsample_bytree': 0.8,
            }
            data_train = lgb.Dataset(self.X_train, self.y_train, categorical_feature=TrainerConfig.categorical_columns)
            cv_results = lgb.cv(
                params, data_train, num_boost_round=10000, nfold=5, stratified=False, shuffle=True, metrics='auc',
                early_stopping_rounds=50, verbose_eval=50, show_stdv=True, seed=i + 10)

            print(f'{i}: best n_estimators:', len(cv_results['auc-mean']))
            print(f'{i}: best cv score:', cv_results['auc-mean'][-1])
            best_n_estimators.append(len(cv_results['auc-mean']))
        print(f"mean:{np.mean(best_n_estimators)} values:\n{best_n_estimators}")

    def find_right_max_depth_num_leaves(self):
        model_lgb = lgb.LGBMClassifier(objective='binary', num_leaves=50,
                                       learning_rate=0.01, n_estimators=85, max_depth=6,
                                       metric='auc', bagging_fraction=0.8, feature_fraction=0.8)

        params_test1 = {
            'max_depth': range(10, 50, 10),
            'num_leaves': range(10, 50, 10)
            # 'num_leaves': [ 3, 4, 5, 6, 7],
        }
        gsearch = GridSearchCV(estimator=model_lgb, param_grid=params_test1, scoring='roc_auc', cv=3, verbose=10, n_jobs=4, refit=True)

        gsearch.fit(self.X_train, self.y_train)

        print(f'参数组:{params_test1}')
        print('参数的最佳取值:{0}'.format(gsearch.best_params_))
        print('最佳模型得分:{0}'.format(gsearch.best_score_))

        logger.info(f"Train AUC ROC:{roc_auc_score(y_true=self.y_train, y_score=gsearch.predict_proba(self.X_train)[:, 1])}")
        logger.info(f"Test AUC ROC:{roc_auc_score(y_true=self.y_test, y_score=gsearch.predict_proba(self.X_test)[:, 1])}")

    def tune(self):
        # stage 1
        # self.find_right_n_estimators()
        # stage 2 TODO: 增加过拟合的部分
        self.find_right_max_depth_num_leaves()

    def get_trained_model(self):
        """

        Returns: trained model

        """
        self.model.fit(self.X_train, self.y_train, categorical_feature=TrainerConfig.categorical_columns, early_stopping_rounds=100, verbose=10, eval_set=(self.X_test, self.y_test))
        logger.info(f"Train AUC ROC:{roc_auc_score(y_true=self.y_train, y_score=self.model.predict_proba(self.X_train)[:, 1])}")
        logger.info(f"Test AUC ROC:{roc_auc_score(y_true=self.y_test, y_score=self.model.predict_proba(self.X_test)[:, 1])}")
        self.model.booster_.save_model(TrainerConfig.lightgbm_model_path)
        self.show_feature()
        return self.model

    def preprocess(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.categorical_encoder.transform()
        df = self.add_label(df)
        processor = Processor()
        df, ma_features = processor.generate_technical_columns(df, "volume_kline_")
        # test ma feature
        self.feature_columns += ma_features
        return df

    @staticmethod
    def add_label(df: pd.DataFrame) -> pd.DataFrame:
        df[TrainerConfig.label_column] = df["Close"].pct_change(-1) * 100 >= 0.1
        return df

    def show_feature(self):
        feature_imp = pd.DataFrame(sorted(zip(self.model.feature_importances_, self.X_train.columns)), columns=['Value', 'Feature'])
        plt.figure(figsize=(20, 10))
        sns.barplot(x="Value", y="Feature", data=feature_imp.sort_values(by="Value", ascending=False).head(30))
        logger.info(f"number of positive importance feature: {feature_imp[feature_imp.Value > 0].shape[0]}")
        logger.info(f"number of total feature: {feature_imp.shape[0]}")
        plt.title('LightGBM Features (avg over folds)')
        plt.tight_layout()
        plt.show()
