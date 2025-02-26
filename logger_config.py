import logging
import colorlog

TRACE_LEVEL = 5

# Add TRACE level to logging
logging.addLevelName(TRACE_LEVEL, "TRACE")

# Add 'trace' method to Logger class
def trace(self, message, *args, **kwargs):
    if self.isEnabledFor(TRACE_LEVEL):
        self._log(TRACE_LEVEL, message, args, **kwargs)

logging.Logger.trace = trace  # Add 'trace' method to Logger class

def setup_logger(log_file='app.log', log_level=logging.DEBUG):
    # Remove default handlers if they exist
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    console_handler = colorlog.StreamHandler()
    console_handler.setFormatter(colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'TRACE': 'white',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'bold_red',
        }
    ))

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s - %(levelname)s - %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S'
    ))

    logger = colorlog.getLogger(__name__)
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    logger.setLevel(log_level)

    return logger
