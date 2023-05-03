import datetime
import inspect
from functools import wraps

from .settings import DEFAULT_HOST, DEFAULT_PORT, DEFAULT_LOGFILE
from .task import _delay, _apply_async
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
    def __init__(self, app_name: str="", broker_url: str="", logfile: str=DEFAULT_LOGFILE):
        self.app_name = app_name
        if not self.app_name:
            raise ValueError("app_name must be set")

        if not broker_url:
            broker_url = f"redis://{DEFAULT_HOST}:{DEFAULT_PORT}/0"

        db = int(broker_url.split('/')[-1])
        host = broker_url.split('/')[2].split(':')[0]
        port = int(broker_url.split('/')[2].split(':')[1])

        self.consumer = _Consumer(db=db, host=host, port=port, app_name=app_name, logfile=logfile)
        self.publisher = _Publisher(db=db, host=host, port=port, app_name=app_name, logfile=logfile)
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

        wrapper.delay = lambda *args, **kwargs: _delay(self.publisher, {
            'app_name': self.app_name,
            'package_name': inspect.getfile(func).split('/')[-2],
            'module_name': inspect.getmodulename(inspect.getfile(func)),
            'function_name': func.__name__,
            'args': args,
            'kwargs': kwargs,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
        wrapper.apply_async = lambda eta, args: _apply_async(self.publisher, {
            'app_name': self.app_name,
            'package_name': inspect.getfile(func).split('/')[-2],
            'module_name': inspect.getmodulename(inspect.getfile(func)),
            'function_name': func.__name__,
            'args': args,
            'timestamp': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }, eta)
        return wrapper

