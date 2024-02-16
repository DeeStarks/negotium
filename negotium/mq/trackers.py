import json
import redis
import uuid

from negotium import conf
from negotium.brokers.main import MessageBroker, BROKER_REDIS

_COMMAND_REDIS_ZREM = 0
_COMMAND_REDIS_LREM = 1
_COMMAND_REDIS_BLPOP = 2

class _MessageTracker:
    """
    A class to track every incoming and outgoing messages
    """
    def __init__(self, broker: MessageBroker, app_name: str):
        self.broker = broker
        self.connection = broker.connect()
        self.app_name = app_name

    def _track(self, command: int, name: str, identifier: str='', uuid_: str='') -> str:
        """Track a message and return the uuid

        Args:
            name (str): name of the queue
            identifier (str): identifier of the message (e.g. a json string value)
            command (int): command to execute
            uuid_ (str): uuid to use (optional: if not provided, a new uuid will be generated)
        """
        uuid_ = uuid_ or str(uuid.uuid4())
        if self.broker.get_broker_name() == BROKER_REDIS:
            self.connection.lpush(conf._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_, json.dumps({
                '_name': name,
                '_identifier': identifier,
                '_command': command
            }))
            return uuid_
        else:
            raise NotImplementedError("Broker not implemented")

    def _delete(self, uuid_: str):
        """Delete a message from the tracker
        """
        if self.broker.get_broker_name() == BROKER_REDIS:
            messages = self.connection.lrange(conf._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_, 0, -1)
            for message in messages:
                data = json.loads(message)
                if data.get('_command') == _COMMAND_REDIS_ZREM:
                    self.connection.zrem(data.get('_name'), data.get('_identifier'))
                elif data.get('_command') == _COMMAND_REDIS_LREM:
                    self.connection.lrem(data.get('_name'), 0, data.get('_identifier'))
                elif data.get('_command') == _COMMAND_REDIS_BLPOP:
                    self.connection.blpop(data.get('_identifier'))
            self.connection.delete(conf._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_)
        else:
            raise NotImplementedError("Broker not implemented")
