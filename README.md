# Negotium

A simple, lightweight, and easy-to-use task or job queue for Python. It tries to mimic the implementation of celery and celery beat, but without the complexity and overhead. For now, it offers only a minimal set of features, which will be expanded. It also currently only supports Redis as the broker; however, it is planned to support other brokers in the future.

## Installation

```bash
pip install negotium
```

## Usage

```python
# ---- main.py (app entry point) ----
from negotium import Negotium

app = Negotium(broker_url="redis://localhost:6379/0", app_name="my_app")
app.start()

@app.task
def add(x, y):
    return x + y

# --- app.py (another module) ----
from main import app

# Run the task asynchronously
add.delay(1, 2)

# Schedule the task to run at a specific time
add.apply_async(args=(1, 2), eta=datetime.datetime.now() + datetime.timedelta(seconds=10))
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

# Set the django settings module
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'my_project.settings')

# Create the negotium app
# If not specified, the broker url defaults to redis://localhost:6379/0
app = Negotium(broker_url="redis://localhost:6379/0", app_name="my_app")
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

def my_async_view(request):
    add.delay(1, 2) # <-- Run the task asynchronously
    return HttpResponse("Hello, world!")

def my_scheduled_view(request):
    add.apply_async(
        args=(1, 2), eta=datetime.datetime.now() + \
            datetime.timedelta(seconds=10)) # <-- Schedule the task to run at a specific time
    return HttpResponse("Hello, world!")
```

You're all set! Now you can run the Django development server and start using negotium.
