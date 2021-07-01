import pandas as pd
from numpy import linspace
from numpy import log10


def get_step(start: float, end: float, n: int):
    """计算步长"""
    step = 10 ** ((log10(end) - log10(start)) / (n - 1)) - 1
    return step


def get_n(start: float, end: float, p: float):
    n = int((log10(end) - log10(start)) / log10(1 + p))
    return n


def get_geometric(start: float, end: float, n: int = None):
    """计算等比数列"""
    li = [start]
    step = get_step(start, end, n)
    for x in range(n - 1):
        start = start * (1 + step)
        li.append(start)
    return li


def get_arithmetic(start: float, end: float, n: int = None):
    """计算等比数列"""

    return linspace(start, end, n)


def get_grid(start_pos: float, end_pos: float, start_price: float, end_price: float, n: int = None, ret: float = None):
    if n is not None:
        pass
    elif ret is not None:
        n = get_n(start_price, end_price, ret)

    pos = get_arithmetic(start=start_pos, end=end_pos, n=n)
    price = get_geometric(start=start_price, end=end_price, n=n)
    df = pd.DataFrame([price, pos]).T
    df.rename(columns={0: 'price', 1: 'position'}, inplace=True)
    return df


if __name__ == '__main__':
    # print(get_geometric(3000, 20000, 50))
    # print(get_geometric(1, 0.1, 50))
    # print(get_arithmetic(1, 0.1, 10))
    print(get_grid(start_pos=1, end_pos=0.1, start_price=3000, end_price=20000, n=50))
