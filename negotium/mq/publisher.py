import datetime
import json
import redis

from negotium.brokers.main import MessageBroker, BROKER_REDIS
from negotium.mq.trackers import _MessageTracker
from negotium.mq.trackers import _COMMAND_REDIS_ZREM, _COMMAND_REDIS_LREM, _COMMAND_REDIS_BLPOP
from negotium.conf import (
    _MESSAGE_MAIN, _MESSAGE_SCHEDULER, _MESSAGE_SCHEDULER_SORTED_SET, _MESSAGE_PERIODIC_TASKS
)
from negotium.schedules.crontab import Crontab
from negotium.utils.logger import log

class _Publisher:
    def __init__(self, broker: MessageBroker, app_name: str, logfile: str=None):
        self.broker = broker
        self.connection = None
        self._tracker = None
        self.app_name = app_name
        self.logfile = logfile

    def _create_connection(self):
        """Create a connection to the message broker
        """
        self.connection = self.broker.connect()
        self._tracker = _MessageTracker(self.broker, self.app_name)

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _publish(self, data: dict, eta: datetime.datetime=None, cron: Crontab=None) -> str:
        """Publish a message to the queue and return the message id
        """
        log(self.logfile, data.get('app_name'), f"Received task: {data.get('function_name')}")
        if self.broker.get_broker_name() == BROKER_REDIS:
            if eta:
                data = {
                    '_task': data,
                    '_eta': eta.strftime('%Y-%m-%d %H:%M:%S.%f')
                }
                self.connection.rpush(_MESSAGE_SCHEDULER + "__" + self.app_name, json.dumps(data))
                message_id = self._tracker._track(
                    name=_MESSAGE_SCHEDULER + "__" + self.app_name, 
                    identifier=json.dumps(data), 
                    command=_COMMAND_REDIS_LREM
                )

                timestamp = datetime.datetime.strptime(eta.strftime('%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f').timestamp()
                self.connection.zadd(_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name, {json.dumps(data): timestamp})
                return self._tracker._track(
                    name=_MESSAGE_SCHEDULER_SORTED_SET + "__" + self.app_name,
                    identifier=json.dumps(data),
                    command=_COMMAND_REDIS_ZREM,
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
                    command=_COMMAND_REDIS_LREM
                )
            else:
                self.connection.rpush(_MESSAGE_MAIN + "__" + self.app_name, json.dumps(data))
                return self._tracker._track(
                    name=_MESSAGE_MAIN + "__" + self.app_name,
                    command=_COMMAND_REDIS_BLPOP
                )
        else:
            raise NotImplementedError("Broker not implemented")
