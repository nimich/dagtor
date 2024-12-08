import logging


class ColorFormatter(logging.Formatter):
    # ANSI escape codes for colors
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"

    def format(self, record):
        if record.levelno == logging.INFO:
            color = self.GREEN
        else:
            color = self.RED
        # Format the log message with color
        message = super().format(record)
        return f"{color}{message}{self.RESET}"


logger = logging.getLogger("main")
logger.propagate = False
logger.setLevel(logging.DEBUG)

# Create a console handler
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

# Set the formatter with color
formatter = ColorFormatter("%(levelname)s: %(message)s")
console_handler.setFormatter(formatter)

logger.addHandler(console_handler)
