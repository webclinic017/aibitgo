import pandas as pd
from datetime import datetime


def combine_two_result(path_a: str, path_b: str):
    df_a = pd.read_csv(path_a)
    df_b = pd.read_csv(path_b)
    df_a["交易方向"] = "多"
    df_b["交易方向"] = "空"
    df = pd.concat([df_a, df_b]).sort_values('开仓时间')
    net_value = 1
    net_values = []
    for index, value in df.iterrows():
        net_values.append(net_value)
        net_value = net_value * (1 + value["盈利"] / 100)

    df['净值'] = net_values
    df.to_csv(f"short_and_long_{datetime.now().minute}_{datetime.now().second}.csv")
    print(f"生成多空成功:{datetime.now()}")
