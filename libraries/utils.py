import logging

def setup_logger(name, log_file, level = logging.INFO):
    """Function to set up a logger with the given name, log file, and level."""
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # File Handler
    file_handler = logging.FileHandler(filename=log_file)
    file_handler.setFormatter(formatter)

    #Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

default_logger = setup_logger('app_logger', 'default.log')