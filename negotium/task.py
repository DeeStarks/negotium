import datetime
import json
import warnings

from negotium.mq.publisher import _Publisher
from negotium.mq.consumer import _Consumer
from negotium.schedules import Crontab
from negotium.conf import _is_worker_enabled, _ignore_execution

def _delay(publisher: _Publisher, data: dict) -> str:
    """Execute the task
    """
    if not _is_worker_enabled():
        if _ignore_execution():
            warnings.warn("The worker is not enabled. The task will be ignored")
            return None
        warnings.warn("The worker is not enabled. The task will be executed synchronously")
        return _Consumer(publisher.broker, publisher.app_name)._execute_task(json.dumps(data))

    publisher._create_connection()
    uuid_ = publisher._publish(data)
    publisher._close_connection()
    return uuid_

def _apply_async(publisher: _Publisher, data: dict, eta: datetime.datetime=None) -> str:
    """Schedule a task to be executed in the future
    """
    if not _is_worker_enabled():
        if _ignore_execution():
            warnings.warn("The worker is not enabled. The task will be ignored")
            return None
        warnings.warn("The worker is not enabled. The task will be executed synchronously")
        return _Consumer(publisher.broker, publisher.app_name)._execute_task(json.dumps(data))

    publisher._create_connection()
    uuid_ = publisher._publish(data, eta=eta)
    publisher._close_connection()
    return uuid_

def _apply_periodic_async(publisher: _Publisher, data: dict, cron: Crontab) -> str:
    """Schedule a periodic task to be executed
    """
    if not _is_worker_enabled():
        if _ignore_execution():
            warnings.warn("The worker is not enabled. The task will be ignored")
            return None
        warnings.warn("The worker is not enabled. The task will be executed synchronously")
        return _Consumer(publisher.broker, publisher.app_name)._execute_task(json.dumps(data))

    publisher._create_connection()
    uuid_ = publisher._publish(data, cron=cron)
    publisher._close_connection()
    return uuid_
