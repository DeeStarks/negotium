import datetime

from negotium.schedules import Crontab
from negotium.mq.publisher import _Publisher

def _delay(publisher: _Publisher, data: dict) -> str:
    """Execute the task
    """
    publisher._create_connection()
    uuid_ = publisher._publish(data)
    publisher._close_connection()
    return uuid_

def _apply_async(publisher: _Publisher, data: dict, eta: datetime.datetime=None) -> str:
    """Schedule a task to be executed in the future
    """
    publisher._create_connection()
    uuid_ = publisher._publish(data, eta=eta)
    publisher._close_connection()
    return uuid_

def _apply_periodic_async(publisher: _Publisher, data: dict, cron: Crontab) -> str:
    """Schedule a periodic task to be executed
    """
    publisher._create_connection()
    uuid_ = publisher._publish(data, cron=cron)
    publisher._close_connection()
    return uuid_
