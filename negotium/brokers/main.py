from typing import Union


BROKER_REDIS = "redis"

class MessageBroker:
    """Abstract class for message brokers."""
    def __init__(self, user: str, password: str, host: str, port: Union[int, str], db: Union[int, str]):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.db = db
        self.broker_name = None

    def connect(self):
        """Connect to the broker, and return the connection object."""
        pass

    def get_broker_name(self):
        """Return the name of the broker."""
        pass
    