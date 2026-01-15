import logging
import sys




class ColorFormatter(logging.Formatter):
    COLORS = {
        "INFO": "\033[92m",
        "WARNING": "\033[93m",
        "ERROR": "\033[91m",
        "DEBUG": "\033[94m",
        "CRITICAL": "\033[95m",
        "RESET": "\033[0m",
    }

    def format(self, record):
        color = self.COLORS.get(record.levelname, "")
        reset = self.COLORS["RESET"]
        record.levelname = f"{color}{record.levelname}{reset}"
        return super().format(record)





def get_logger(name: str):
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    handler = logging.StreamHandler(sys.stdout)
    formatter = ColorFormatter("[%(asctime)s] [%(levelname)s] %(name)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    return logger
