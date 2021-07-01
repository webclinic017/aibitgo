import asyncio

from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR, EVENT_JOB_MISSED
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from api.basis import Basis
from base.config import logger_level
from base.consts import WeComAgent, WeComUser
from base.log import Logger
from timer.asynexchange import AsynExchange
from timer.fund_rate import FundRateClass
from util.wecom_message_util import WeComMessage

logger = Logger('scheduler', logger_level)


def my_listener(event):
    """
    定时任务是否成功执行
    """
    job = scheduler.get_job(event.job_id)
    if event.exception:
        msg = f"定时任务出错:{event.scheduled_run_time.strftime('%Y-%m-%d %H:%M:%S')}:{job.name}:{event.exception}"
        wc = WeComMessage(msg=msg, agent=WeComAgent.scheduler, touser=[WeComUser.John])
        logger.info(asyncio.run(wc.send_text()))
    else:
        logger.info(f"定时任务执行成功:{event.scheduled_run_time.strftime('%Y-%m-%d %H:%M:%S')}:{job.name}")


# scheduler = BlockingScheduler()
scheduler = AsyncIOScheduler()

scheduler.add_job(
    func=AsynExchange.update_symbol, id='get_all_symbols', name='更新symbol，定时任务，每天下午4:00-4:15进行',
    trigger='cron', hour=16, minute='4-15', replace_existing=True, coalesce=True)

scheduler.add_job(
    func=AsynExchange.update_symbol, id='get_all_symbols_per_hour', name='更新symbol，定时任务,每小时一次',
    trigger='cron', minute='0-3', replace_existing=True, coalesce=True)

scheduler.add_job(
    func=Basis.get_max_min, args=[1], id="basis_get_max_min_1days", name='更新基差1D最大值最小值，间隔任务，5分钟一次',
    trigger='interval', minutes=5, jitter=50, replace_existing=True, coalesce=True)

scheduler.add_job(
    func=Basis.get_max_min, args=[7], id="basis_get_max_min_7days", name='更新基差7D最大值最小值，间隔任务，1小时一次',
    trigger='interval', hours=1, jitter=100, replace_existing=True, coalesce=True)

scheduler.add_job(
    func=Basis.get_max_min, args=[30], id="basis_get_max_min_30days", name='更新基差30D最大值最小值，间隔任务，3小时一次',
    trigger='interval', hours=3, jitter=300, replace_existing=True, coalesce=True)

scheduler.add_job(
    func=Basis.cal_all_basis, args=[True], id="cal_all_basis", name='基差入库，间隔任务，1分钟一次',
    trigger='interval', minutes=1, jitter=10, replace_existing=True, coalesce=True)

scheduler.add_job(
    func=AsynExchange.update_total_balance, id="update_total_balance", name='资金快照入库，间隔任务，1分钟一次',
    trigger='interval', minutes=1, jitter=10, replace_existing=True, coalesce=True)

scheduler.add_job(
    func=FundRateClass.get_history_fund_rate, id='get_history_fund_rate', name='更新历史资金费率，每天下午0:01 8：01 16：01进行',
    trigger='cron', hour='0,8,16', minute=1, replace_existing=True, coalesce=True)

# scheduler.add_job(
#     func=fund_robot, id='fund_robot', name='更新历史资金费率，每天下午23:55 7：55 15：55进行',
#     trigger='cron', hour='23,7,15', minute=55, replace_existing=True, coalesce=True)

scheduler.add_listener(callback=my_listener, mask=EVENT_JOB_ERROR | EVENT_JOB_MISSED | EVENT_JOB_EXECUTED)


def start_scheduler():
    AsynExchange.update_symbol()
    Basis.get_max_min(1)
    Basis.get_max_min(7)
    Basis.get_max_min(30)
    scheduler.start()


if __name__ == '__main__':
    logger.info('启动定时任务')
    scheduler.start()
    asyncio.get_event_loop().run_forever()
