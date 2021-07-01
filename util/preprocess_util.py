import pandas as pd
from tqdm import tqdm

from base.config import logger
from base.consts import PreProcessConfig
from util.df_util import get_column_info, get_column_frequent


class Processor(object):
    def __init__(self):
        self.volumn_technical_columns = []
        self.kline_technical_columns = []

    @staticmethod
    def generate_technical_columns(df, prefix: str, columns=('Open', 'Close', 'Volume'), windows=(10, 20, 60)):
        """均线相关

        Args:
            df: 分钟级别的K线数据
            prefix: label的前缀
            columns: 计算均线的列
            windows: 计算均线的时间

        Returns:

        """
        technical_columns = []
        ma_columns = []
        ma_dev_columns = []
        drop_list = []
        for col in columns:
            for window in windows:
                ma_column = prefix + 'ma_{0}_{1}'.format(col, window)
                ma_dev_column = prefix + 'ma_dev_{0}_{1}'.format(col, window)
                ma_lag_column = prefix + 'ma_lag_{0}_{1}'.format(col, window)
                ma_lag_rate_column = prefix + 'ma_lag_rate_{0}_{1}'.format(col, window)
                std_column = prefix + 'std_{0}_{1}'.format(col, window)
                ma_columns.append(ma_column)
                ma_dev_columns.append(ma_dev_column)
                # calc moving average
                df[ma_column] = df[col].rolling(window).mean()
                # calc rate of deviation from moving average
                df[ma_dev_column] = df[col] / df[ma_column] - 1
                # calc moving std
                df[std_column] = df[col].rolling(window).std()

                df[ma_lag_column] = df[ma_column].shift()
                df[ma_lag_rate_column] = (df[ma_column] - df[ma_lag_column]) / df[ma_lag_column]

                technical_columns += [ma_dev_column, std_column, ma_lag_rate_column]

                drop_list.append(ma_column)
                drop_list.append(ma_lag_column)

                # df.drop(drop_list, axis=1, inplace=True)
                logger.info(f"K线加上ma处理的后的缺失值:\n{df.isnull().sum()}")
        return df, technical_columns

    @staticmethod
    def fill_kline_missing(df: pd.DataFrame) -> pd.DataFrame:
        """把分钟级K线数据填满

        Args:
            df: 分钟级K线数据

        Returns:

        """
        start = df.index.min()
        end = df.index.max()
        dates = pd.date_range(start=start, end=end, freq='T')
        df = df.reindex(dates)
        # 填补缺失的数据，forward fill
        df.fillna(method='ffill', inplace=True)
        return df

    def merge_all_df(self, df: pd.DataFrame, gold_df: pd.DataFrame, jinse_df: pd.DataFrame, twitter_whale_df: pd.DataFrame) -> pd.DataFrame:
        """把数据合并处理为分钟级的K线数据

        Args:
            df: 分钟级K线数据
            gold_df: 黄金价格数据
            jinse_df: 处理过金色财经数据
            twitter_whale_df: 处理过的twitter转账数据

        Returns:
            pd.DataFrame

        """
        logger.info("开始把数据合并到一起,分钟级")

        logger.info("检查数据缺失:")
        logger.info(
            "K线:\n" + str(
                df.isnull().sum()
            )
        )
        logger.info(
            "转账:\n" + str(
                twitter_whale_df.isnull().sum()
            )
        )

        # 整理金色财经的数据
        # 处理缺失值
        jinse_df.type.fillna("普通", inplace=True)
        jinse_df.first_tag.fillna("unknown", inplace=True)
        logger.info(
            "金色财经:\n" + str(
                jinse_df.isnull().sum()
            )
        )

        # 生成分钟级别的时间用于merge
        jinse_df['time'] = jinse_df.index
        jinse_df['time'] = jinse_df.time.dt.ceil("T")

        # 填满分钟级的K线
        df = self.fill_kline_missing(df)

        # 添加分钟级别K线的技术指标
        df, self.kline_technical_columns = self.generate_technical_columns(df, "minute_kline_")

        # 重新设置K线的index
        df['candle_begin_time'] = df.index
        df.reset_index(inplace=True)

        # 合并分钟级K线和金色财经数据
        result = pd.merge(jinse_df, df, right_on=["candle_begin_time"], left_on=["time"], how='outer')

        # 确认合并之后数据没有缺失
        assert result[~result.long_index.isnull()].shape[0] == jinse_df.shape[0]

        # 整理twitter的数据
        twitter_whale_df = twitter_whale_df.add_prefix("twitter_")
        twitter_whale_df["time"] = twitter_whale_df.index
        twitter_whale_df["time"] = twitter_whale_df["time"].dt.ceil("T")
        result = pd.merge(twitter_whale_df, result, right_on=["candle_begin_time"], left_on=["time"], how='outer')

        # 确认是时间顺序的
        result.sort_values('candle_begin_time', inplace=True)
        assert result.candle_begin_time.is_monotonic_increasing

        return result

    def merge_info_volume(self, df: pd.DataFrame, gold_df: pd.DataFrame, jinse_df: pd.DataFrame, twitter_whale_df: pd.DataFrame, is_volume_index: bool = True) -> pd.DataFrame:
        """把数据合并处理为Volume based K线数据

        Args:
            is_volume_index: 是否使用基于交易量的数据
            df: 分钟级K线数据
            gold_df: 黄金价格数据
            jinse_df: 处理过金色财经数据
            twitter_whale_df: 处理过的twitter转账数据

        Returns:
            pd.DataFrame

        """
        # 把所有数据聚合为按照时间为顺序的分钟级数据
        df = self.merge_all_df(df=df, gold_df=gold_df, jinse_df=jinse_df, twitter_whale_df=twitter_whale_df)
        logger.info(df.shape)
        logger.info("总数据的缺失值\n" + str(df.isnull().sum()))

        # 如果不是基于交易量的K线数据可以直接返回
        if not is_volume_index:
            df.to_csv(PreProcessConfig.MINUTE_RESULT_PATH)
            logger.info(df)
            return df

        if PreProcessConfig.DEBUG:
            logger.critical("在测试模式，只有1000条数据")
            df = df.tail(1000)
            logger.info(df)

        logger.info("开始把数据合并到一起,基于交易量...")
        # 显示pandas的进度
        df["Volumes"] = df["Volume"].apply(lambda x: [1 for x in range(int(x))])
        df = df.explode("Volumes")
        del df["Volumes"]
        df["Volume"] = 1

        # 对交易量求和并转为时间
        df['sum_volume'] = df['Volume'].cumsum() + 2147483647
        df['sum_volume_time'] = pd.to_datetime(df['sum_volume'], unit='s')

        df = df.reindex()
        df.set_index(['sum_volume_time'], inplace=True)

        # 聚合数据
        logger.info(f"开始聚合数据,数据量:{df.shape[0]}")
        # 显示pandas的进度
        tqdm.pandas()
        df = df.groupby(pd.Grouper(freq=f'{PreProcessConfig.RESAMPLE_VOLUME}S')).progress_apply(self.preprocess_period)

        # df.set_index('candle_begin_time', inplace=True)

        # 最后删除还为Nan的数据
        logger.info(f"删除空值前数据的数量:{df.shape}")
        df = df.round(4)
        df.to_csv(PreProcessConfig.VOLUME_RESULT_PATH)
        df.dropna(inplace=True)
        logger.info(df)
        logger.info(f"删除空值后数据的数量:{df.shape}, 结果的缺失值:\n{df.isnull().sum()}")
        logger.info(f"聚合数据成功!结果文件保存在:{PreProcessConfig.VOLUME_RESULT_PATH}")
        return df

    def preprocess_period(self, x: pd.DataFrame) -> pd.Series:
        """统计一段时间内的数据

        Args:
            x: 一段时间内的数据, 根据交易量生成的

        Returns:
            统计后的结果

        """
        # basic OLCH
        open = x.Open[0]
        low = x.Low.min()
        close = x.Close[-1]
        high = x.High.max()
        volume = x.Volume.sum()
        # 需要用最后的时间，因为是在那个时间交易量才满足了
        candle_begin_time = x.candle_begin_time[-1]

        # price related
        max_change = high - low

        # time related
        dayofweek = x.candle_begin_time[-1].dayofweek
        hour = x.candle_begin_time[-1].hour

        # jinse related
        jinse_long_index = x.long_index.sum()
        jinse_short_index = x.short_index.sum()
        jinse_comment_number = x.comment_number.sum()
        jinse_max_long_index = get_column_info(x, "long_index", "min")
        jinse_max_short_index = get_column_info(x, "short_index", "min")
        jinse_max_comment_number = get_column_info(x, "comment_number", "max")
        jinse_min_long_index = get_column_info(x, "long_index", "min")
        jinse_min_short_index = get_column_info(x, "short_index", "min")
        jinse_min_comment_number = get_column_info(x, "comment_number", "min")
        jinse_news_number = len(x.content_score.unique()) - 1
        jinse_max_tag_number = get_column_info(x, "tag_number", "max")
        jinse_max_title_words = get_column_info(x, "title_words", "max")
        jinse_min_title_words = get_column_info(x, "title_words", "min")
        jinse_max_title_score = get_column_info(x, "title_score", "max")
        jinse_min_title_score = get_column_info(x, "title_score", "min")
        jinse_max_content_words = get_column_info(x, "content_words", "max")
        jinse_min_content_words = get_column_info(x, "content_words", "min")
        jinse_max_content_score = get_column_info(x, "content_score", "max")
        jinse_min_content_score = get_column_info(x, "content_score", "min")

        # 开始处理twitter转账数据
        twitter_whale_number = len(x.twitter_unique_key.unique()) - 1
        twitter_usd_number_min, twitter_usd_number_max, twitter_usd_number_sum = get_column_info(x, "twitter_usd_number", "all")
        twitter_coin_amount_min, twitter_coin_amount_max, twitter_coin_amount_sum = get_column_info(x, "twitter_coin_amount", "all")
        twitter_favorite_count_min, twitter_favorite_count_max, twitter_favorite_count_sum = get_column_info(x, "twitter_favorite_count", "all")
        twitter_retweet_count_min, twitter_retweet_count_max, twitter_retweet_count_sum = get_column_info(x, "twitter_retweet_count", "all")
        twitter_light_number_max = get_column_info(x, "twitter_light_number", "max")
        twitter_light_number_sum = get_column_info(x, "twitter_light_number", "sum")

        # 开始处理所有的categorical数据
        # twitter 转账
        twitter_tag = get_column_frequent(x, "twitter_tag")
        twitter_to_addr = get_column_frequent(x, "twitter_to_addr")
        twitter_from_addr = get_column_frequent(x, "twitter_from_addr")
        twitter_transfer_coin_name = get_column_frequent(x, "twitter_transfer_coin_name")

        # jinse
        jinse_type = get_column_frequent(x, "type")
        jinse_note = get_column_frequent(x, "note")
        jinse_first_tag = get_column_frequent(x, "first_tag")
        jinse_has_link = get_column_frequent(x, "has_link")

        # how long it takes
        duration = (x.candle_begin_time[-1] - x.candle_begin_time[0]).total_seconds() / 60

        # hardcode columns
        result = pd.Series(
            [
                open,
                low,
                close,
                high,
                volume,
                max_change,
                candle_begin_time,
                dayofweek,
                hour,

                # jinse related
                jinse_long_index,
                jinse_short_index,
                jinse_comment_number,
                jinse_max_long_index,
                jinse_max_short_index,
                jinse_news_number,
                jinse_max_tag_number,
                jinse_max_title_words,
                jinse_min_title_words,
                jinse_max_title_score,
                jinse_min_title_score,
                jinse_max_content_words,
                jinse_min_content_words,
                jinse_max_content_score,
                jinse_min_content_score,
                jinse_max_comment_number,
                jinse_min_long_index,
                jinse_min_short_index,
                jinse_min_comment_number,

                # twitter related
                twitter_whale_number,
                twitter_usd_number_min,
                twitter_usd_number_max,
                twitter_usd_number_sum,
                twitter_coin_amount_min,
                twitter_coin_amount_max,
                twitter_coin_amount_sum,
                twitter_favorite_count_min,
                twitter_favorite_count_max,
                twitter_favorite_count_sum,
                twitter_retweet_count_min,
                twitter_retweet_count_max,
                twitter_retweet_count_sum,
                twitter_light_number_max,
                twitter_light_number_sum,

                # categorical
                twitter_tag,
                twitter_to_addr,
                twitter_from_addr,
                twitter_transfer_coin_name,
                jinse_type,
                jinse_note,
                jinse_first_tag,
                jinse_has_link,

                duration
            ], index=[
                "Open",
                "Low",
                "Close",
                "High",
                "Volume",
                "max_change",
                "candle_begin_time",
                "dayofweek",
                "hour",

                # jinse related
                "jinse_long_index",
                "jinse_short_index",
                "jinse_comment_number",
                "jinse_max_long_index",
                "jinse_max_short_index",
                "jinse_news_number",
                "jinse_max_tag_number",
                "jinse_max_title_words",
                "jinse_min_title_words",
                "jinse_max_title_score",
                "jinse_min_title_score",
                "jinse_max_content_words",
                "jinse_min_content_words",
                "jinse_max_content_score",
                "jinse_min_content_score",
                "jinse_max_comment_number",
                "jinse_min_long_index",
                "jinse_min_short_index",
                "jinse_min_comment_number",

                # twitter related
                "twitter_whale_number",
                "twitter_usd_number_min",
                "twitter_usd_number_max",
                "twitter_usd_number_sum",
                "twitter_coin_amount_min",
                "twitter_coin_amount_max",
                "twitter_coin_amount_sum",
                "twitter_favorite_count_min",
                "twitter_favorite_count_max",
                "twitter_favorite_count_sum",
                "twitter_retweet_count_min",
                "twitter_retweet_count_max",
                "twitter_retweet_count_sum",
                "twitter_light_number_max",
                "twitter_light_number_sum",

                # categorical
                "twitter_tag",
                "twitter_to_addr",
                "twitter_from_addr",
                "twitter_transfer_coin_name",
                "jinse_type",
                "jinse_note",
                "jinse_first_tag",
                "jinse_has_link",
                "duration"
            ]
        )
        # 取分钟级别K线的技术指标的最后一个数值
        for column in self.kline_technical_columns + [x for x in x.columns.tolist() if x.startswith("eth_")]:
            result[column] = x[column][-1]

        return result
