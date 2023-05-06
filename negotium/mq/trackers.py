import json
import redis
import uuid

from negotium import settings

_COMMAND_ZREM = 0
_COMMAND_LREM = 1
_COMMAND_BLPOP = 2

class _MessageTracker:
    """
    A class to track every incoming and outgoing messages
    """
    def __init__(self, connection: redis.Redis, app_name: str):
        self.connection = connection
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
        self.connection.lpush(settings._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_, json.dumps({
            '_name': name,
            '_identifier': identifier,
            '_command': command
        }))
        return uuid_

    def _delete(self, uuid_: str):
        """Delete a message from the tracker
        """
        messages = self.connection.lrange(settings._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_, 0, -1)
        for message in messages:
            data = json.loads(message)
            if data.get('_command') == _COMMAND_ZREM:
                self.connection.zrem(data.get('_name'), data.get('_identifier'))
            elif data.get('_command') == _COMMAND_LREM:
                self.connection.lrem(data.get('_name'), 0, data.get('_identifier'))
            elif data.get('_command') == _COMMAND_BLPOP:
                self.connection.blpop(data.get('_identifier'))
        self.connection.delete(settings._MESSAGE_TRACKER + "__" + self.app_name + "__" + uuid_)
