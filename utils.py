import os
from dotenv import load_dotenv
import logging

def is_docker() -> bool:
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )


if is_docker() is False:  # Use .env file for secrets if outside of a container
    load_dotenv()

LOG_LEVEL = os.getenv('LOG_LEVEL') or 'INFO'

class CustomFormatter(logging.Formatter):  # Formatter

    grey = "\x1b[38;20m"
    blue = "\x1b[34m"
    yellow = "\x1b[33;20m"
    red = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset = "\x1b[0m"
    format = """%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"""

    FORMATS = {
        logging.DEBUG: grey + format + reset,
        logging.INFO: blue + format + reset,
        logging.WARNING: yellow + format + reset,
        logging.ERROR: red + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        dt_fmt = '%m/%d/%Y %I:%M:%S'
        formatter = logging.Formatter(log_fmt, dt_fmt)
        return formatter.format(record)
    

# log = logging.getLogger()
# log.setLevel(LOG_LEVEL)
# handler = logging.StreamHandler()
# handler.setFormatter(CustomFormatter())
# log.addHandler(handler)


log = logging.getLogger()
log.setLevel(LOG_LEVEL)
handler = logging.StreamHandler()
handler.setFormatter(CustomFormatter())
log.addHandler(handler)
