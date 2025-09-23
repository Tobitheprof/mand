import time
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def timed(fn):
    @wraps(fn)
    def _inner(*args, **kwargs):
        start = time.perf_counter()
        try:
            return fn(*args, **kwargs)
        finally:
            dur = (time.perf_counter() - start) * 1000
            logger.info(f"{fn.__name__} completed", extra={"duration_ms": round(dur, 2)})
    return _inner
