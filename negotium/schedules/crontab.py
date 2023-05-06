class Crontab:
    """Crontab class.

    This class is used to create a crontab object that can be used to
    schedule tasks to be executed in a periodic manner.

    Example:
        from negotium.schedules import Crontab

        # create a crontab for every January 1st at midnight
        crontab = Crontab(month=1, day=1, hour=0, minute=0)

        # create a crontab for every 1st day of the month at midnight
        crontab = Crontab(day=1, hour=0, minute=0)

        # create a crontab for every Monday at midnight
        crontab = Crontab(weekday=0, hour=0, minute=0)

        # create a crontab for every midnight
        crontab = Crontab(hour=0, minute=0)

        # create a crontab for every 5 minutes
        crontab = Crontab(minute=5)

        # create a crontab for every 5 seconds
        crontab = Crontab(second=5)

        # using a crontab expression
        crontab = Crontab(expression='1 * * * *') # <-- every hour at minute 1
    """ 
    def __init__(self, month: int=None, day: int=None, weekday: int=None, hour: int=None, minute: int=None, expression: str=None):
        if not month and not day and not weekday and not hour and not minute and not expression:
            raise ValueError('provide at least one argument')
        if expression:
            self.expression = expression
        else:
            self._parse_init(month, day, weekday, hour, minute)

    def __repr__(self):
        return self.expression

    def __str__(self):
        return self.expression

    def _parse_init(self, month: int=None, day: int=None, weekday: int=None, hour: int=None, minute: int=None):
        """Convert the arguments to a crontab expression
        """
        self.expression = "%s %s %s %s %s" % (
            minute if minute is not None else '*',
            hour if hour is not None else '*',
            day if day is not None else '*',
            month if month is not None else '*',
            weekday if weekday is not None else '*'
        )
