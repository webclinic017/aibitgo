import time
import webbrowser

from base.config import logger
from util.kline_volume_util import volume_df

try:
    from ._version import version as __version__  # noqa: F401
except ImportError:
    pass  # Package not installed
from datetime import datetime, timedelta
from pprint import pformat
from typing import Type, Callable, Optional

import numpy as np
import pandas as pd

from api.base_api import Direction, OrderType, BacktestDetailType
from base.consts import BacktestConfig
from db.model import StrtategyBackTestIndexModel, StrtategyBackTestDetailModel
from util.kline_util import get_kline
from . import lib  # noqa: F401
from ._plotting import set_bokeh_output  # noqa: F401
from .backtesting import Backtest, Strategy  # noqa: F401
from tqdm import tqdm


def save_trade_record(trade: pd.DataFrame, test_id: int, note: pd.DataFrame, equity: pd.DataFrame) -> None:
    """保存回测的交易记录

    Args:
        trade:  所有的交易记录 backtest_result._trades
        test_id: 某次回测的id
        note: 回测的信号标注
        equity: 回测的详细数据

    Returns:
        None

    """
    logger.info("开始保存回测交易结果...")
    results = []
    for row in tqdm(trade.itertuples(index=False, name="Trade"), total=trade.shape[0]):
        # 开仓
        current = equity[equity.index >= row.EntryTime].iloc[0]
        e = equity[equity.index >= row.EntryTime].iloc[1]
        if note.shape[0] > 0 and note[note.index == row.EntryTime].shape[0] > 0:
            n = note[note.index == row.EntryTime].iloc[0]
        else:
            n = pd.DataFrame([{"detail": ""}]).iloc[0]
        open_data = {
            "detail_type": BacktestDetailType.TRADE,
            "test_id": test_id,
            "timestamp": row.EntryTime.to_pydatetime() + timedelta(hours=8),
            "order_side": Direction.OPEN_LONG if row.Size > 0 else Direction.OPEN_SHORT,
            "order_type": OrderType.MARKET,
            "order_amount": abs(row.Size),
            "order_price": float(row.EntryPrice),
            "position_amount": 0 if np.isnan(e.Position) else float(e.Position),
            "position_direction": get_direction(e.Position),
            "price": float(current.Close),
            "equity": float(e.Equity / BacktestConfig.TOTAL_CASH),
            "note": n.detail
        }
        results.append(open_data)

        # 平仓
        current = equity[equity.index >= row.ExitTime].iloc[0]
        e = equity[equity.index >= row.ExitTime].iloc[1]
        if note.shape[0] > 0 and note[note.index == row.EntryTime].shape[0] > 0:
            n = note[note.index == row.EntryTime].iloc[0]
        else:
            n = pd.DataFrame([{"detail": ""}]).iloc[0]
        close_data = {
            "detail_type": BacktestDetailType.TRADE,
            "test_id": test_id,
            "timestamp": row.ExitTime.to_pydatetime() + timedelta(hours=8),
            "order_side": Direction.CLOSE_LONG if row.Size > 0 else Direction.CLOSE_SHORT,
            "order_type": OrderType.MARKET,
            "order_amount": abs(row.Size),
            "order_price": row.ExitPrice,
            "order_pnl": float(row.PnL / BacktestConfig.TOTAL_CASH),
            "position_amount": 0 if np.isnan(e.Position) else float(e.Position),
            "position_direction": get_direction(e.Position),
            "price": float(current.Close),
            "equity": float(e.Equity / BacktestConfig.TOTAL_CASH),
            "note": n.detail
        }
        results.append(close_data)

    # 把数据改成pandas DataFrame 用来处理成一分钟一笔
    if len(results) > 0:
        df = pd.DataFrame(results).set_index("timestamp").resample(rule='1T').aggregate(
            {"detail_type": "last", "test_id": "last", "order_side": "last", "order_type": "last", "order_amount": "sum", "order_price": "last", "position_amount": "last", "price": "last", "equity": "last",
             "note": "last",
             "order_pnl": "last"}).dropna(subset=["test_id"])
        df["timestamp"] = df.index.tolist()
        for data in df.to_dict(orient='record'):
            try:
                data['timestamp'] = data['timestamp'].to_pydatetime()
                data["order_pnl"] = clean_numeric(data["order_pnl"])
                StrtategyBackTestDetailModel.create_data(data)
            except Exception as e:
                logger.warning(f"发现异常数据{data}:{e}")
    else:
        logger.warning("没有交易结果数据")
    logger.info("保存回测交易结果成功!")


def save_snapshot_record(df: pd.DataFrame, test_id: int, note: pd.DataFrame, timeframe: str = "1d") -> None:
    """保存回测的快照数据

    Args:
        df: 资金曲线 backtest_result._equity_curve
        test_id: 某次回测的id
        timeframe: 时间纬度 1d/15T/1T

    Returns:
        None

    """
    logger.info("开始保存回测快照结果...")
    if note.shape[0] > 0:
        df = pd.concat([df, note], axis=1, join='outer')
    else:
        df["detail"] = ""
    row = df.iloc[0]
    data = {
        "detail_type": BacktestDetailType.SNAPSHOT,
        "test_id": test_id,
        "timestamp": row.name.to_pydatetime() + timedelta(hours=8),
        "position_amount": 0 if np.isnan(row.Position) else row.Position,
        "position_direction": get_direction(row.Position),
        "price": float(row.Close),
        "equity": float(row.Equity / BacktestConfig.TOTAL_CASH),
        "note": str(row.detail)
    }
    StrtategyBackTestDetailModel.create_data(data)
    resampled_df = df.resample(timeframe, label="right", closed="left").aggregate(
        "last"
    )
    for row in tqdm(resampled_df.itertuples(index=True, name="Record"), total=resampled_df.shape[0]):
        try:
            data = {
                "detail_type": BacktestDetailType.SNAPSHOT,
                "test_id": test_id,
                "timestamp": row.Index.to_pydatetime() + timedelta(hours=8),
                "position_amount": 0 if np.isnan(row.Position) else row.Position,
                "position_direction": get_direction(row.Position),
                "price": row.Close,
                "equity": float(row.Equity / BacktestConfig.TOTAL_CASH),
                "note": str(row.detail)
            }
            StrtategyBackTestDetailModel.create_data(data)
        except Exception as e:
            logger.error(f"保存回测快照失败!\n{e}")

    logger.info("保存回测快照成功!")


def run_backtest(
        strategy: Type[Strategy],
        custom_data: Optional[pd.DataFrame] = None,
        symbol_id: int = 0,
        start_time: str = "2019-10-01 00:00:00",
        end_time: str = "2020-10-01 00:00:00",
        timeframe="1m",
        commission=.001,
        is_basis: bool = False,
        slippage=.001,
        exclusive_orders=False,
        strategy_id: int = 1,
        leverage: float = 1,
        detail: str = False,
        basis: Optional[pd.DataFrame] = None,
        factor_df: Optional[pd.DataFrame] = None,
        volume_kline=False,
        fixed_commission=None,
) -> pd.Series:
    """运行一次回测

    Args:
        custom_data: 自己定义的data
        is_basis: 是否是基差算法
        volume_kline: 基于交易量的K线
        factor_df: 因子数据
        basis: 基差数据
        strategy: 策略类
        symbol_id:  交易对ID
        start_time: 回测开始时间 2019-10-01
        end_time:  回测结束时间 2020-09-01
        timeframe: 回测数据频率 1m/15m/30m
        commission: 手续费
        slippage: 滑点
        strategy_id: 策略id 如果不传入会自动生成
        detail: 策略结果的入库级别 ，1m/15m/30m/1h/1day
        leverage:  杠杆倍数,
        fixed_commission: 固定的交易手续费，当这个为固定值时，不会启用comission
        exclusive_orders:

    Returns: 回测的结果
    """
    if isinstance(custom_data, pd.DataFrame):
        logger.info("正在使用自定义的数据进行回测")
        data = custom_data
    elif isinstance(basis, pd.DataFrame):
        logger.info("正在使用基差数据进行回测")
        data = basis
    elif isinstance(volume_kline, pd.DataFrame):
        logger.info("正在使用交易量的K线数据进行回测")
        data = volume_df(symbol_id, volume=250)
    else:
        data = get_kline(symbol_id, start_time, end_time, timeframe)

    # 检查因子数据
    if isinstance(factor_df, pd.DataFrame) and factor_df.shape[0]:
        if factor_df.shape[0] != data.shape[0]:
            logger.warning("The shape of two dataframe is not the same!")
        data = pd.concat([data, factor_df], axis=1, sort=True)

    logger.info("回测运行开始")
    backtest_start_time = time.time()
    bt = Backtest(data=data, strategy=strategy, commission=commission + slippage, exclusive_orders=exclusive_orders, margin_rate=float(1 / leverage), is_basis=is_basis)
    backtest_result = bt.run()
    ExecutionTime = int(time.time() - backtest_start_time)
    try:
        if np.isnan(backtest_result["Max. Drawdown Duration"]):
            max_drawdown_duration = 0
        elif isinstance(backtest_result["Max. Drawdown Duration"], float):
            max_drawdown_duration = int(backtest_result["Max. Drawdown Duration"])
        else:
            max_drawdown_duration = backtest_result["Max. Drawdown Duration"].days
    except Exception as e:
        max_drawdown_duration = 0

    index_data = {
        "OptimizeName": "backtest",
        "Symbol_id": symbol_id,
        "Commission": commission,
        "Slippage": slippage,
        "Strategy_id": strategy_id,
        "Param": {k: v for k, v in strategy.__dict__.items() if ((isinstance(v, int) or isinstance(v, bool) or isinstance(v, str) or isinstance(v, datetime)) and k != "__module__")},
        "Start": data.index[0].to_pydatetime(),
        "End": data.index[-1].to_pydatetime(),
        "Returns": backtest_result["Return [%]"],
        "BaseReturns": backtest_result["Buy & Hold Return [%]"],
        "MaxReturns": 0,
        "AnnualizedReturns": 0,
        "Alpha": 0,
        "Beta": 0,
        "AlgorithmVolatility": 0,
        "BenchmarkVolatility": 0,
        "Sharpe": clean_numeric(backtest_result["Sharpe Ratio"]),
        "Sortino": clean_numeric(backtest_result["Sortino Ratio"]),
        "MaxDrawdown": clean_numeric(backtest_result["Max. Drawdown [%]"]),
        "MaxDrawdownDuration": max_drawdown_duration,
        # "MaxDrawdownDuration": 0,
        "TradeTimes": clean_numeric(backtest_result["# Trades"]),
        "WinRate": clean_numeric(backtest_result["Win Rate [%]"]),
        "ProfitCossRatio": 0,
        "BestTrade": clean_numeric(backtest_result["Best Trade [%]"]),
        "WorstTrade": clean_numeric(backtest_result["Worst Trade [%]"]),
        "SQN": clean_numeric(backtest_result["SQN"]),
        "ExecutionTime": ExecutionTime
    }
    logger.info(f"回测运行成功!结果如下:\n{pformat(index_data, indent=20)}")
    backtest_index = StrtategyBackTestIndexModel.create_data(index_data)
    logger.info("保存回测基础结果成功!")
    if detail:
        logger.info("开始保存回测详细结果")
        if hasattr(backtest_result._strategy, 'note'):
            note = pd.DataFrame(backtest_result._strategy.note)
            if len(backtest_result._strategy.note) > 0:
                note.set_index([0], inplace=True)
                note.columns = ["detail"]
        save_trade_record(trade=backtest_result._trades, test_id=backtest_index.id, note=note, equity=backtest_result._equity_curve)
        save_snapshot_record(df=backtest_result._equity_curve, test_id=backtest_index.id, timeframe=detail, note=note)
        logger.info("开始保存回测详细成功!")
    logger.info(f"回测运行成功！本次回测的回测ID为:http://192.168.3.2/strategy/backtest/detail?id={backtest_index.id}")
    webbrowser.open(f"http://192.168.3.2/strategy/backtest/detail?id={backtest_index.id}")
    return backtest_index.id


def run_optimize(
        strategy: Type[Strategy],
        symbol_id: int,
        start_date: str = "2019-10-01 00:00:00",
        end_date: str = "2020-10-01 00:00:00",
        timeframe="1m",
        constraint: Callable[[dict], bool] = None,
        maximize: str = 'Equity Final [$]',
        commission=.0005,
        slippage=.0005,
        exclusive_orders=True,
        **kwargs
) -> pd.Series:
    """ 运行优化

    Args:
        strategy: 策略类
        symbol_id:  交易对ID
        start_date: 回测开始时间 2019-10-01
        end_date:  回测结束时间 2020-09-01
        timeframe: 回测数据频率 1m/15m/30m
        commission: 手续费
        slippage: 滑点
        constraint: 参数的限制 constraint=lambda param: param.n1 < param.n2
        maximize: 要最大化的目标 'Equity Final [$]'
        exclusive_orders:

    Returns:
        优化的结果

    """
    logger.info("参数优化开始")
    bt = Backtest(data=get_kline(symbol_id, start_date, end_date, timeframe), strategy=strategy, commission=commission + slippage, exclusive_orders=exclusive_orders)
    result = bt.optimize(
        maximize=maximize, constraint=constraint,
        **kwargs,
    )
    logger.info("参数优化结束")
    return result


def clean_numeric(data):
    if np.isnan(data):
        return 0
    return data


def get_direction(position: float):
    if position > 0:
        direction = "long"
    elif position < 0:
        direction = "short"
    else:
        direction = ""
    return direction
