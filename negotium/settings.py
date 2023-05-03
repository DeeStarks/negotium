"""This module contains the settings for the negotium application.
"""

# default settings
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 6379
DEFAULT_USERNAME = 'guest'
DEFAULT_PASSWORD = 'guest'
DEFAULT_QUEUE = 'negotium_queue'
DEFAULT_SCHEDULER_QUEUE = 'negotium_scheduler_queue'
DEFAULT_SCHEDULER_SORTED_SET = 'negotium_scheduler_sorted_set'
DEFAULT_LOGFILE = 'negotium.log'

# logging settings
LOGGING_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s\n'
LOGGING_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
