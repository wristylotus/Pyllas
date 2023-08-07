import logging


def get_logger(name=__name__, log_level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(log_level)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    formatter = logging.Formatter('\n%(asctime)s - %(name)s - %(levelname)s:\n%(message)s')
    console_handler.setFormatter(formatter)
    logger.handlers = [console_handler]

    return logger
