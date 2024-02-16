import datetime
import inspect
from functools import wraps

from .conf import DEFAULT_HOST, DEFAULT_PORT
from .task import _delay, _apply_async, _apply_periodic_async
from negotium.brokers.main import MessageBroker
from negotium.mq.consumer import _Consumer
from negotium.mq.publisher import _Publisher
from negotium.utils.logger import log


class Negotium:
    """This class is the entry point to the negotium application.

    Negotium currently supports Redis as the message broker. This class
    provides a decorator to mark functions as tasks and a method to consume
    tasks from the message broker.

    Example:
        from negotium import Negotium

        negotium = Negotium(app_name="test_app", broker_url="redis://localhost:6379/0")
        negotium.start()

        @negotium.task
        def add(x, y):
            return x + y

    Note: This class should be instantiated at the entry point of your application.
    """
    def __init__(self, app_name: str, broker: MessageBroker, logfile: str=None):
        self.app_name = app_name
        if not self.app_name:
            raise ValueError("app_name must be set")

        if not broker.get_broker_name():
            raise ValueError("invalid broker")

        self.consumer = _Consumer(broker=broker, app_name=app_name, logfile=logfile)
        self.publisher = _Publisher(broker=broker, app_name=app_name, logfile=logfile)
        self.logfile = logfile

    def start(self, *args, **kwargs):
        """Use this method to consume tasks from the message broker

        Example:
            negotium.start()

        Note: This method should be called at the entry point of your application.
        """
        self.consumer.run()

    def close(self, *args, **kwargs):
        """Use this method to close the connection to the message broker

        Example:
            negotium.close()

        Note: This method should be called at the exit point of your application.
        """
        self.consumer.close()

    def task(self, func):
        """Decorator for task functions

        Example:
            @negotium.task
            def add(x, y):
                return x + y
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        d = {
            'app_name': self.app_name,
            'package_dir': '/'.join(inspect.getfile(func).split('/')[:-1]),
            'package_name': inspect.getfile(func).split('/')[-2],
            'module_name': inspect.getmodulename(inspect.getfile(func)),
            'function_name': func.__name__,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        wrapper.delay = lambda *args, **kwargs: _delay(self.publisher, {**d, 'args': args, 'kwargs': kwargs})
        wrapper.apply_async = lambda eta, args: _apply_async(self.publisher, {**d,'args': args}, eta)

        def apply_periodic_async(cron, args):
            uuid_ = _apply_periodic_async(self.publisher, {**d, 'args': args}, cron)
            # restart the consumer to pick up the new periodic task
            self.consumer._consume_periodic_tasks()
            return uuid_
        wrapper.apply_periodic_async = apply_periodic_async
        return wrapper

    def cancel(self, uuid_: str):
        """Cancel a scheduled task

        Example:
            negotium.cancel(uuid_)
        """
        self.consumer._delete_message(uuid_)
