import logging
_logger = None
def create_logger(level=logging.INFO):
    global _logger
    if _logger is None:
        logger = logging.getLogger(__name__)
        logger.setLevel(level)

        handler = logging.StreamHandler()
        logger.addHandler(handler)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        _logger = logger

    return _logger