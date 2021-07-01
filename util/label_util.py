import os

import pandas as pd
from snownlp import SnowNLP

from db.model import Factor, NewsFactor
from util.df_util import query2df
from base.config import BASE_DIR, logger


def add_max_change_in_range(df: pd.DataFrame, time_window: int = 10, column_name: str = "Close", max_column=True
                            ) -> pd.DataFrame:
    origin_columns = df.columns.tolist()
    history_columns = []
    for i in range(1, time_window + 1):
        shift_name = f"{column_name}_after_{i}"
        df[shift_name] = df[column_name].shift(-i)
        history_columns.append(shift_name)
    df.dropna(inplace=True)
    df["max"] = df[history_columns].max(axis=1)
    df["max_change"] = ((df["max"] - df[column_name]) / df[column_name]) * 100
    df["max_change"] = df["max_change"].round(2)
    origin_columns.append("max_change")
    if max_column:
        df["max_column"] = df[history_columns].idxmax(axis=1)
        origin_columns.append("max_column")
    return df[origin_columns]


def preprocess_jinse(df: pd.DataFrame) -> pd.DataFrame:
    """整理金色财经的数据
    """
    columns_name = ['long_index', 'short_index', 'comment_number', 'type', 'note']
    # nlp
    logger.info("jinse,开始处理title数据...")
    df['title_snow'] = df['title'].apply(lambda x: SnowNLP(x))
    df['title_words'] = df['title_snow'].apply(lambda x: len(x.words))
    df['title_score'] = df['title_snow'].apply(lambda x: x.sentiments)
    logger.info("jinse.开始处理content数据...")
    df['content_snow'] = df['content'].apply(lambda x: SnowNLP(x))
    df['content_words'] = df['content_snow'].apply(lambda x: len(x.words))
    df['content_score'] = df['content_snow'].apply(lambda x: x.sentiments)
    columns_name += ['title_words', 'title_score', 'content_words', 'content_score']

    logger.info("jinse.开始处理其他数据...")
    # tag process
    df['first_tag'] = df['tag'].apply(lambda x: x.split("-")[0])
    df['tag_number'] = df['tag'].apply(lambda x: len(x.split("-")) - 1)
    columns_name += ['first_tag', 'tag_number']
    #  has link
    df["has_link"] = df["link"] != ""
    columns_name += ["has_link"]
    return df[columns_name]


def set_time_index(df: pd.DataFrame, col="timestamp"):
    df[col] = pd.to_datetime(df[col])
    df.set_index(col, inplace=True)
    return df


def get_jinse(start_time: str, end_time: str, use_cache=True) -> pd.DataFrame:
    filename = f"jinse__{start_time}___{end_time}.csv".replace(" ", "-")
    cache_path = os.path.join(BASE_DIR, "cache", filename)
    if os.path.isfile(cache_path) and use_cache:
        logger.info(f"从缓存中读取jinse数据: {cache_path}...")
        df = pd.read_csv(cache_path, index_col=0, parse_dates=True, infer_datetime_format=True)
        logger.info(f"从缓存中读取jinse数据成功")
        return df

    jinse = NewsFactor.get_news(source="jinse", start_date=start_time, end_date=end_time)
    jinse_df = set_time_index(query2df(jinse), col="news_time")
    jinse_df = preprocess_jinse(jinse_df)
    jinse_df.name = "jinse"
    jinse_df.to_csv(cache_path)
    return jinse_df


def get_twitter(start_time: str, end_time: str) -> pd.DataFrame:
    twitter = Factor.get_factors(source="twitter_whale", start_date=start_time, end_date=end_time)
    twitter_df = set_time_index(query2df(twitter))
    twitter_df.name = "twitter_whale"
    return twitter_df


def get_chaindd(start_time: str, end_time: str) -> pd.DataFrame:
    chaindd = Factor.get_factors(source="chaindd", start_date=start_time, end_date=end_time)
    chaindd_df = set_time_index(query2df(chaindd))
    chaindd_df.name = "chaindd"
    return chaindd_df
