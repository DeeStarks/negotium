import datetime

from negotium.mq.publisher import _Publisher

def _delay(publisher: _Publisher, data: dict):
    """Execute the task
    """
    publisher._create_connection()
    publisher._publish(data)
    publisher._close_connection()

def _apply_async(publisher: _Publisher, data: dict, eta: datetime.datetime=None):
    """Schedule a task to be executed in the future
    """
    publisher._create_connection()
    publisher._publish(data, eta=eta)
    publisher._close_connection()
