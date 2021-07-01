"""DataFrame Related
"""
from typing import Iterable, Union, Tuple

import pandas as pd


def query2df(query: Iterable) -> pd.DataFrame:
    """convert sqlalchemy query to pandas DataFrame

    Args:
        query:  sqlalchemy query

    Returns:
        DataFrame

    """
    df = pd.DataFrame([x.to_dict() for x in query])
    # 清理掉一些数据库里面的数据
    columns = df.columns.tolist()
    drop_columns = []
    for col in ["id", "create_time", "update_time", "is_delete", "source"]:
        if col == "id" and "unique" in columns:
            drop_columns.append(col)
        elif col in columns:
            drop_columns.append(col)
    df.drop(columns=drop_columns, inplace=True)
    return df


def get_column_info(df: pd.DataFrame, column_name: str, target: str):
    """

    Args:
        df
        column_name: 对应的列名
        target: max,min,sum

    Returns:

    """
    result = 0
    if target == "max":
        result = df[column_name].max()

    elif target == "min":
        result = df[column_name].min()
    elif target == "sum":
        result = df[column_name].sum()
    elif target == "all":
        return get_column_info(df, column_name, "max"), get_column_info(df, column_name, "min"), get_column_info(df, column_name, "sum")

    # filter nan
    if pd.isna(result):
        return 0

    return result


def get_column_frequent(df: pd.DataFrame, column_name: str):
    """返回这一列里面出现次数最多的值

    Args:
        df:
        column_name: 对应的列名

    Returns:

    """
    if df[column_name].isnull().sum() == df.shape[0]:
        if df[column_name].dtype == "object":
            return "unknown"
        elif df[column_name].dtype == "float64":
            return 0.0

    most_frequent_value = df[column_name].value_counts().nlargest(n=1).index[0]
    return most_frequent_value
