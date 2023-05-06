"""This module contains the settings for the negotium application.
"""

# default settings
DEFAULT_HOST = 'localhost'
DEFAULT_PORT = 6379
DEFAULT_USERNAME = 'guest'
DEFAULT_PASSWORD = 'guest'
DEFAULT_LOGFILE = 'negotium.log'

# message settings
_MESSAGE_MAIN = 'negotium_queue'
_MESSAGE_SCHEDULER = 'negotium_scheduler_queue'
_MESSAGE_SCHEDULER_SORTED_SET = 'negotium_scheduler_sorted_set'
_MESSAGE_TRACKER = 'negotium_tracker'
_MESSAGE_PERIODIC_TASKS = 'negotium_periodic_tasks'

# logging settings
LOGGING_FORMAT = '[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s\n'
LOGGING_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'
