import datetime
import json
import redis

from negotium.settings import DEFAULT_QUEUE, DEFAULT_SCHEDULER_QUEUE, DEFAULT_SCHEDULER_SORTED_SET
from negotium.utils.logger import log

class _Publisher:
    def __init__(self, db: int, host: str, port: int, logfile: str=None):
        self.connection = None
        self.logfile = logfile
        self.db = db
        self.host = host
        self.port = port

    def _create_connection(self):
        """Create a connection to the message broker
        """
        self.connection = redis.Redis(db=self.db, host=self.host, port=self.port)

    def _close_connection(self):
        """Close the connection
        """
        self.connection.close()

    def _publish(self, data: dict, eta: datetime.datetime=None):
        """Publish a message to the queue
        """
        log(self.logfile, data.get('app_name'), f"Received task: {data.get('function_name')}")
        if eta:
            data = {
                '_task': data,
                '_eta': eta.strftime('%Y-%m-%d %H:%M:%S.%f')
            }
            self.connection.rpush(DEFAULT_SCHEDULER_QUEUE, json.dumps(data))
            # zadd
            timestamp = datetime.datetime.strptime(eta.strftime('%Y-%m-%d %H:%M:%S.%f'), '%Y-%m-%d %H:%M:%S.%f').timestamp()
            self.connection.zadd(DEFAULT_SCHEDULER_SORTED_SET, {json.dumps(data): timestamp})
        else:
            self.connection.rpush(DEFAULT_QUEUE, json.dumps(data))
