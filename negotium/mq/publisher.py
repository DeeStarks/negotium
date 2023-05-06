import datetime
import json
import redis

from negotium.mq.trackers import _MessageTracker
from negotium.mq.trackers import _COMMAND_ZREM, _COMMAND_LREM, _COMMAND_BLPOP
from negotium.settings import (
    _MESSAGE_MAIN, _MESSAGE_SCHEDULER, _MESSAGE_SCHEDULER_SORTED_SET, _MESSAGE_PERIODIC_TASKS
)
from negotium.schedules.crontab import Crontab
from negotium.utils.logger import log

class _Publisher:
    def __init__(self, db: int, host: str, port: int, app_name: str, logfile: str=None):
        self.connection = None
        self._tracker = None
        self.db = db
        self.host = host
        self.port = port
        self.app_name = app_name
        self.logfile = logfile

    def _create_connection(self):
        """Create a connection to the message broker
        """
        self.connection = redis.Redis(db=self.db, host=self.host, port=self.port)
        self._tracker = _MessageTracker(self.connection, self.app_name)

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _publish(self, data: dict, eta: datetime.datetime=None, cron: Crontab=None) -> str:
        """Publish a message to the queue and return the message id
        """
        log(self.logfile, data.get('app_name'), f"Received task: {data.get('function_name')}")
        if eta:
            data = {
                '_task': data,
                '_eta': eta.strftime('%Y-%m-%d %H:%M:%S.%f')
            }
            self.connection.rpush(_MESSAGE_SCHEDULER + "__" + self.app_name, json.dumps(data))
            message_id = self._tracker._track(
                name=_MESSAGE_SCHEDULER + "__" + self.app_name, 
                identifier=json.dumps(data), 
                command=_COMMAND_LREM
            )

            timestamp = datetime.datetime.strptime(eta.strftime('%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f').timestamp()
            self.connection.zadd(_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name, {json.dumps(data): timestamp})
            return self._tracker._track(
                name=_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name,
                identifier=json.dumps(data),
                command=_COMMAND_ZREM,
                uuid_=message_id
            )
        elif cron:
            data = {
                '_task': data,
                '_cron': cron.__str__()
            }
            self.connection.rpush(_MESSAGE_PERIODIC_TASKS + "__" + self.app_name, json.dumps(data))
            return self._tracker._track(
                name=_MESSAGE_PERIODIC_TASKS + "__" + self.app_name,
                identifier=json.dumps(data), 
                command=_COMMAND_LREM
            )
        else:
            self.connection.rpush(_MESSAGE_MAIN + "__" + self.app_name, json.dumps(data))
            return self._tracker._track(
                name=_MESSAGE_MAIN + "__" + self.app_name,
                command=_COMMAND_BLPOP
            )

