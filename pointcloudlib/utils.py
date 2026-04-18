import logging
import time
from contextlib import contextmanager
from functools import wraps
from pathlib import Path

logger = logging.getLogger(__name__)
logger.addHandler(logging.NullHandler())


def open_in_cloudcompare(dataset_path: Path | str) -> None:
    import os
    import subprocess

    if os.path.exists(dataset_path):
        logger.info(f"Opening {dataset_path} in CloudCompare...")
        cmd = ["flatpak", "run", "org.cloudcompare.CloudCompare", str(dataset_path)]

        subprocess.Popen(cmd)
    else:
        logger.warning(f"Could not find file at {dataset_path}.")


def timed(task_name: str):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start = time.perf_counter()
            result = func(*args, **kwargs)
            elapsed = time.perf_counter() - start
            logger.info(f"{task_name} took {elapsed:.3f}s")
            return result

        return wrapper

    return decorator


@contextmanager
def status_spinner(message: str):
    logger.info(message)
    yield
    logger.info("Done.")
