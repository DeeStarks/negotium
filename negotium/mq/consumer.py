import croniter
import datetime
import redis
import inspect
import importlib
import json
import os
import signal
import sys
import time
from threading import Thread, Timer

from negotium.brokers.main import MessageBroker, BROKER_REDIS
from negotium.mq.trackers import _MessageTracker
from negotium.conf import (
    _MESSAGE_MAIN, _MESSAGE_SCHEDULER, _MESSAGE_SCHEDULER_SORTED_SET, _MESSAGE_PERIODIC_TASKS
)
from negotium.utils.logger import log


class _Consumer:
    def __init__(self, broker: MessageBroker, app_name: str, logfile: str=None):
        self.broker = broker
        self.connection = broker.connect()
        self._tracker = _MessageTracker(broker, app_name)
        self._is_closed = False
        self.app_name = app_name
        self.logfile = logfile
        self._thread_consume = None
        self._thread_consume_scheduled = None
        self._thread_consume_periodic = None

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _delete_message(self, uuid_: str):
        """Delete a message
        """
        self._tracker._delete(uuid_)

    def _consume(self, *args, **kwargs):
        """Consume messages from the queue
        """
        if self.broker.get_broker_name() == BROKER_REDIS:
            while True:
                if self._is_closed:
                    return
                message = self.connection.blpop(_MESSAGE_MAIN + "__" + self.app_name)
                self._callback(message[1])
                time.sleep(1)
        else:
            raise NotImplementedError("Broker not implemented")

    def _consume_scheduled_tasks(self, *args, **kwargs):
        """Load scheduled tasks
        """
        if self.broker.get_broker_name() == BROKER_REDIS:
            while True:
                if self._is_closed:
                    return
                current_time = datetime.datetime.now().timestamp()
                tasks = self.connection.zrangebyscore(_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name, 0, current_time)
                for task in tasks:
                    task = json.loads(task.decode('utf-8'))
                    eta = task.get('_eta')
                    task = task.get('_task')
                    self.connection.zrem(_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name, json.dumps({
                        '_task': task,
                        '_eta': eta
                    }))
                    # execute task
                    self._callback_scheduled(json.dumps(task), eta)
                time.sleep(1)
        else:
            raise NotImplementedError("Broker not implemented")

    def _consume_periodic_tasks(self, *args, **kwargs):
        """Load periodic tasks
        """
        tasks = []
        if self.broker.get_broker_name() == BROKER_REDIS:
            tasks = self.connection.lrange(_MESSAGE_PERIODIC_TASKS + "__" + self.app_name, 0, -1)
        else:
            raise NotImplementedError("Broker not implemented")

        for task in tasks:
            body = json.loads(task.decode('utf-8'))
            cron = body.get('_cron')
            task = body.get('_task')

            # schedule task with the given cron
            croniter_ = croniter.croniter(cron, datetime.datetime.now())
            self._schedule_periodic_task(croniter_, task, cron)

    def _schedule_periodic_task(self, croniter_: croniter.croniter, task: dict, cron: str):
        """Execute a periodic task at the next cron
        """
        eta = croniter_.get_next(datetime.datetime)
        delay = (eta - datetime.datetime.now()).total_seconds()
        Timer(delay, self._callback_periodic, [json.dumps(task), cron]).start()

    def _callback(self, body):
        """Callback function
        """
        self._execute_task(body)

    def _callback_scheduled(self, body, eta):
        """Callback function for scheduled tasks
        """
        self._execute_task(body)
        self.connection.lrem(_MESSAGE_SCHEDULER + "__" + self.app_name, 0, json.dumps({
            '_task': json.loads(body),
            '_eta': eta
        }))

    def _callback_periodic(self, body, cron):
        """Callback function for periodic tasks
        """
        self._execute_task(body)

    def _execute_task(self, body):
        """Execute a task
        """
        # log the message
        # extract dict from bytes
        body = json.loads(body)

        # get function arguments
        app_name = body.get('app_name')
        package_dir = body.get('package_dir')
        package_name = body.get('package_name')
        module_name = body.get('module_name')
        function_name = body.get('function_name')
        args = body.get('args', [])
        kwargs = body.get('kwargs', {})
        is_scheduled = body.get('_is_scheduled')
        log(self.logfile, app_name, 
            f"{'[Scheduled] ' if is_scheduled else ''}Executing (task: {function_name})", level="INFO")
        
        # import
        spec = importlib.util.spec_from_file_location(f"{module_name}.{function_name}", f"{package_dir}/{module_name}.py")
        module = importlib.util.module_from_spec(spec)
        sys.modules[f"{module_name}.{function_name}"] = module
        spec.loader.exec_module(module)
        # execute the function
        function = getattr(module, function_name)
        try:
            res = function(*args, **kwargs)
            log(self.logfile, app_name, 
                f"{'[Scheduled] ' if is_scheduled else ''}Result (task: {function_name}): {res}", level="INFO")
            return res
        except Exception as e:
            log(self.logfile, app_name, 
                f"{'[Scheduled] ' if is_scheduled else ''}Error (task: {function_name}): {e}", level="ERROR")

    def run(self):
        """Run the consumers in a separate threads
        """
        # create threads
        self._thread_consume = Thread(target=self._consume, daemon=True)
        self._thread_consume_scheduled = Thread(target=self._consume_scheduled_tasks, daemon=True)
        self._thread_consume_periodic = Thread(target=self._consume_periodic_tasks, daemon=True)
        # start threads
        self._thread_consume.start()
        self._thread_consume_scheduled.start()
        self._thread_consume_periodic.start()

    def close(self):
        """Close the connection
        """
        self._is_closed = True
        self._close_connection()
