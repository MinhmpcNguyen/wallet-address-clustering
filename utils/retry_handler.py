import time
from functools import wraps

from utils.logger_utils import get_logger

logger = get_logger('Retry handler')


def retry_handler(retries_number: int = 0,
                  sleep_time: float = 0.1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            _retry_time = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    logger.error(f'Error at {func.__name__}: {ex}')
                    _retry_time += 1
                    if retries_number and _retry_time >= retries_number:
                        raise ex
                    time.sleep(sleep_time)
        return wrapper
    return decorator
