# Negotium

A simple, lightweight, and easy-to-use task or job queue for Python. It tries to mimic the implementation of celery and celery beat, but without the complexity and overhead. It runs on a single machine, and it is designed to be used in a single process.

For now, it offers only a minimal set of features, which will be expanded. It also currently only supports Redis as the broker; however, it is planned to support other brokers in the future.

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
