import logging

def init_logger() -> logging.Logger:
    l = logging.getLogger(__name__)
    console_handler = logging.StreamHandler()
    l.addHandler(console_handler)
    l.setLevel(logging.DEBUG)
    return l
