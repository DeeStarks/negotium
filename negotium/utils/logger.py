from datetime import datetime

from negotium.settings import LOGGING_FORMAT, LOGGING_DATE_FORMAT

def log(logfile: str, app_name: str, message: str, level: str="INFO"):
    """Log a message to a file

    Args:
        app_name (str): The name of the application
        message (str): The message to log
        level (str): The logging level
    """
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
