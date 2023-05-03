import datetime
import redis
import inspect
import importlib
import json
import signal
import sys
import time
from multiprocessing import Process

from negotium.settings import DEFAULT_QUEUE, DEFAULT_SCHEDULER_QUEUE, DEFAULT_SCHEDULER_SORTED_SET
from negotium.utils.logger import log


class _Consumer:
    def __init__(self, db: int, host: str, port: int, logfile: str=None):
        self.connection = redis.Redis(db=db, host=host, port=port)
        self.logfile = logfile

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _consume(self, *args, **kwargs):
        """Consume messages from the queue
        """
        while True:
            message = self.connection.blpop(DEFAULT_QUEUE)
            self._callback(message[1])

    def _consume_scheduled_tasks(self, *args, **kwargs):
        """Load scheduled tasks
        """
        while True:
            current_time = datetime.datetime.now().timestamp()
            # get the tasks
            tasks = self.connection.zrangebyscore(DEFAULT_SCHEDULER_SORTED_SET, 0, current_time)
            # loop through the tasks
            for task in tasks:
                # get the task
                task = json.loads(task.decode('utf-8'))
                # get the eta
                eta = task.get('_eta')
                # get the task
                task = task.get('_task')
                # remove the task from the sorted set
                self.connection.zrem(DEFAULT_SCHEDULER_SORTED_SET, json.dumps({
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
        self.connection.lrem(DEFAULT_SCHEDULER_QUEUE, 0, json.dumps({
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
        # create a new process
        p = Process(target=self._consume)
        # start the process
        p.start()
        # create a new process
        p2 = Process(target=self._consume_scheduled_tasks)
        # start the process
        p2.start()

    def close(self):
        """Close the connection
        """
        self._close_connection()
