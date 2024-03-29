import logging
import logging.handlers


def init_logging(loglvl, filepath=None):
    # [%(name)s (%(process)d)]
    format_detail = '%(asctime)s %(levelname)s: %(message)s'
    formatter = logging.Formatter(format_detail)

    # TODO: how to reset loggers? where to call it?
    _logger = logging.getLogger('disco')
    _logger.setLevel(loglvl)

    # File logger
    if filepath:
        file_handler = logging.FileHandler(filepath)
        file_handler.setLevel(loglvl)
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)

    # Console logger
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(loglvl)
    stream_handler.setFormatter(formatter)
    _logger.addHandler(stream_handler)

    #'ext://flask.logging.wsgi_errors_stream'
    #_logger.addHandler(logging.NullHandler())
