import time
from datetime import datetime, timedelta
import threading

lock = threading.Lock()

def seconds_until_midnight():
    now = datetime.now()
    tomorrow = now + timedelta(minutes=30)
    midnight = datetime(tomorrow.year, tomorrow.month, tomorrow.day)
    return (midnight - now).total_seconds()

def job(func, *args, **kwargs):
    if not lock.acquire(blocking=False):
        print("Previous run still active. Skipping...")
        return

    try:
        print("Running job...")
        func(*args, **kwargs)
        print("Job done.")
    finally:
        lock.release()


