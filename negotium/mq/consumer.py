import datetime
import redis
import inspect
import importlib
import json
import os
import signal
import time
from multiprocessing import Process

from negotium.settings import DEFAULT_QUEUE, DEFAULT_SCHEDULER_QUEUE, DEFAULT_SCHEDULER_SORTED_SET
from negotium.utils.logger import log


class _Consumer:
    def __init__(self, db: int, host: str, port: int, app_name: str, logfile: str=None):
        self.connection = redis.Redis(db=db, host=host, port=port)
        self._is_closed = False
        self.app_name = app_name
        self.logfile = logfile
        self._process_consume = None
        self._process_consume_scheduled = None

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _consume(self, *args, **kwargs):
        """Consume messages from the queue
        """
        while True:
            # check if connection is closed
            if self._is_closed:
                return
            message = self.connection.blpop(DEFAULT_QUEUE + "__" + self.app_name)
            self._callback(message[1])
            # sleep for 1 second
            time.sleep(1)

    def _consume_scheduled_tasks(self, *args, **kwargs):
        """Load scheduled tasks
        """
        while True:
            # check if connection is closed
            if self._is_closed:
                return
            current_time = datetime.datetime.now().timestamp()
            # get the tasks
            tasks = self.connection.zrangebyscore(DEFAULT_SCHEDULER_SORTED_SET + "__" + self.app_name, 0, current_time)
            # loop through the tasks
            for task in tasks:
                # get the task
                task = json.loads(task.decode('utf-8'))
                # get the eta
                eta = task.get('_eta')
                # get the task
                task = task.get('_task')
                # remove the task from the sorted set
                self.connection.zrem(DEFAULT_SCHEDULER_SORTED_SET + "__" + self.app_name, json.dumps({
                    '_task': task,
                    '_eta': eta
                }))
                # execute the task
                self._callback_scheduled(json.dumps(task), eta)
            # sleep for 1 second
            time.sleep(1)

    def _callback(self, body):
        """Callback function
        """
        self._execute_task(body)

    def _callback_scheduled(self, body, eta):
        """Callback function for scheduled tasks
        """
        self._execute_task(body)
        # remove the task from the queue
        self.connection.lrem(DEFAULT_SCHEDULER_QUEUE + "__" + self.app_name, 0, json.dumps({
            '_task': json.loads(body),
            '_eta': eta
        }))

    def _execute_task(self, body):
        """Execute a task
        """
        # log the message
        # extract dict from bytes
        body = json.loads(body)

        # get function arguments
        app_name = body.get('app_name')
        package_name = body.get('package_name')
        module_name = body.get('module_name')
        function_name = body.get('function_name')
        args = body.get('args', [])
        kwargs = body.get('kwargs', {})
        is_scheduled = body.get('_is_scheduled')
        log(self.logfile, app_name, 
            f"{'[Scheduled] ' if is_scheduled else ''}Executing (task: {function_name})", level="INFO")
        
        # import the module
        module = importlib.import_module(f"{package_name}.{module_name}")
        # get the function
        function = getattr(module, function_name)
        # execute the function
        try:
            res = function(*args, **kwargs)
            log(self.logfile, app_name, 
                f"{'[Scheduled] ' if is_scheduled else ''}Result (task: {function_name}): {res}", level="INFO")
        except Exception as e:
            log(self.logfile, app_name, 
                f"{'[Scheduled] ' if is_scheduled else ''}Error (task: {function_name}): {e}", level="ERROR")

    def run(self):
        """Run the consumers in a separate process
        """
        # create processes
        self._process_consume = Process(target=self._consume)
        self._process_consume_scheduled = Process(target=self._consume_scheduled_tasks)
        # start processes
        self._process_consume.start()
        self._process_consume_scheduled.start()

        # register a signal handler
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # wait for both processes to finish
        # p.join()
        # p2.join()

    def _signal_handler(self, sig, frame):
        """Handle signals
        """
        # close the connection
        self.close()
        # exit
        os._exit(1)

    def close(self):
        """Close the connection
        """
        self._is_closed = True
        self._close_connection()

        # terminate processes
        if self._process_consume and self._process_consume.is_alive():
            self._process_consume.terminate()
        if self._process_consume_scheduled and self._process_consume_scheduled.is_alive():
            self._process_consume_scheduled.terminate()
