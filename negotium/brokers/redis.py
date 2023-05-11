import redis

from typing import Union
from .main import MessageBroker, BROKER_REDIS


class Redis(MessageBroker):
    """Redis message broker."""
    def __init__(self, host: str, port: Union[int, str], db: int=0, user: str="", password: str=""):
        super().__init__(user, password, host, port, db)
        self.broker_name = BROKER_REDIS

    def connect(self) -> redis:
        """Connect to Redis."""
        return redis.Redis(
            host=self.host, port=self.port, db=self.db, username=self.user, password=self.password)

    def get_broker_name(self) -> str:
        return self.broker_name
