from threading import Lock
import time

_warm_status = {
    "running": False,
    "phase": "",
    "message": "",
    "percent": 0,
    "started_at": None,
    "updated_at": None,
}

_lock = Lock()

def set_warm_status(**kwargs):
    with _lock:
        _warm_status.update(kwargs)
        _warm_status["updated_at"] = time.time()

def get_warm_status():
    with _lock:
        return dict(_warm_status)

def start_warm():
    set_warm_status(
        running=True,
        percent=0,
        started_at=time.time(),
        message="Starting..."
    )

def finish_warm():
    set_warm_status(
        running=False,
        percent=100,
        message="Complete"
    )
