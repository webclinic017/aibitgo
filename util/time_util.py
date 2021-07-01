from datetime import datetime


class TimeUtil(object):
    format_str = "%Y-%m-%d %H:%M:%S"

    @classmethod
    def format_time(cls, time: datetime) -> str:
        return time.strftime(cls.format_str)
