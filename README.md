# Negotium

A simple, lightweight, and easy-to-use task/job queue for Python. It provides a simple API for running tasks asynchronously, scheduling tasks to run at a specific time, and running tasks periodically.

Unlike celery, negotium is designed to be simple and easy to use. It does not require a separate worker process to run tasks. Instead, it uses a single-threaded event loop to run tasks asynchronously. This makes it ideal for small to medium-sized projects that do not require the complexity of a full-blown task queue.

> Negotium supports only Redis as a broker for now. Support for other brokers will be added in the future.

## Installation

```bash
pip install negotium
```

## Features

- Asynchronous task execution
- Scheduled task execution
- Dynamic periodic task execution
- Task cancellation: All tasks are cancellable using the UUID returned by the task execution methods: `delay`, `apply_async`, and `apply_periodic_async`

## Usage

```python
# ---- main.py (app entry point) ----
from negotium import Negotium
from negotium.brokers import Redis

# create broker
broker = Redis(
    host='localhost',
    port=6379,
    user='default', # optional
    password='password', # optional
    db=0 # optional (defaults to 0)
)

# create negotium app
app = Negotium(
    app_name="<YOUR_APP_NAME>", 
    broker=broker,
    log_file="<PATH_TO_LOG_FILE>" # optional. Defaults to stdout
)
app.start()

@app.task
def add(x, y):
    return x + y
```

#### Delayed task execution

```python
add.delay(1, 2)
```

#### Scheduled task execution

```python
add.apply_async(args=(1, 2), eta=datetime.datetime.now() + datetime.timedelta(seconds=10))
```

#### Dynamic periodic task execution

> Note: Periodic tasks are scheduled using `negotium.schedules.Crontab` object. The `Crontab` object takes the following arguments:
> - `minute`: The minute to run the task at
> - `hour`: The hour to run the task at
> - `day`: The day of the week to run the task at
> - `month`: The day of the month to run the task at
> - `weekday`: The month of the year to run the task at
> 
> Or, you can pass a raw crontab expression as a string. For example: `* * * * *` will run the task every minute.


```python
from main import app
from negotium.schedules import Crontab

# run the task every minute
task_id = add.apply_periodic_async(args=(1, 2), cron=Crontab(minute=1))

# with a raw crontab expression
task_id = add.apply_periodic_async(args=(1, 2), cron=Crontab(expression="* * * * *"))

# to cancel, call the `cancel` method
app.cancel(task_id)
```

### Using in a Django project

- Create a `negotium.py` file in your Django project directory
```
my_project/
    manage.py
    my_project/
        __init__.py
        settings.py
        urls.py
        negotium.py # <-- Create this file
        wsgi.py
```

- Add the following code to the `negotium.py` file
```python
import os

from negotium import Negotium
from negotium.brokers import Redis

# Set the django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_project.settings')

# Create the broker
broker = Redis(
    host='localhost',
    port=6379,
    user='default', # optional
    password='password', # optional
    db=0 # optional (defaults to 0)
)

# Create the negotium app
app = Negotium(app_name="<YOUR_APP_NAME>", broker=broker)
app.start()
```

- Import the `app` object in your task modules
```python
# --- example/tasks.py ---
from my_project.negotium import app

@app.task
def add(x, y):
    return x + y
```

- In your views, you can run the task asynchronously
```python
# --- example/views.py ---
from example.tasks import add
from negotium.schedules import Crontab

def my_async_view(request):
    add.delay(1, 2) # <-- Run the task asynchronously
    return HttpResponse("Hello, world!")

def my_scheduled_view(request):
    add.apply_async( # <-- Schedule the task to run at a specific time
        args=(1, 2), eta=datetime.datetime.now() + datetime.timedelta(seconds=10)
    )
    return HttpResponse("Hello, world!")

def my_periodic_view(request):
    add.apply_periodic_async(args=(1, 2), cron=Crontab(expression="* * * * *"))
    return HttpResponse("Hello, world!")
```

You're all set! Now you can run the Django development server and start using negotium.
