import logging.handlers
import sys

from constants import LOG_FILE

app_logger = logging.getLogger('app')
app_logger.setLevel(logging.DEBUG)

fh = logging.handlers.RotatingFileHandler(LOG_FILE)
fh.setLevel(logging.DEBUG)
fh.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

app_logger.addHandler(fh)

# remove if logging to the console is not needed
app_logger.addHandler(logging.StreamHandler(sys.stdout))
