from datetime import datetime

from negotium.conf import LOGGING_FORMAT, LOGGING_DATE_FORMAT

def log(logfile: str, app_name: str, message: str, level: str="INFO"):
    """Log a message to a file if a logfile is provided.
    If a logfile is not provided, the message is logged to stdout.

    Args:
        app_name (str): The name of the application
        message (str): The message to log
        level (str): The logging level
    """
    if logfile:
        # create a log file
        log_file = open(logfile, 'a')
        # write the message to the log file
        log_file.write(LOGGING_FORMAT % {
            'asctime': datetime.now().strftime(LOGGING_DATE_FORMAT),
            'name': app_name,
            'levelname': level,
            'message': message
        })
        # close the log file
        log_file.close()
    else:
        # log to stdout
        print(LOGGING_FORMAT.replace('\n', '') % {
            'asctime': datetime.now().strftime(LOGGING_DATE_FORMAT),
            'name': app_name,
            'levelname': level,
            'message': message
        })
