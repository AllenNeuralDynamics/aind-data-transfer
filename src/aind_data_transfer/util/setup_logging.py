import logging
import logging.config
import socket


class HostnameFilter(logging.Filter):
    hostname = socket.gethostname()

    def filter(self, record):
        record.hostname = self.hostname
        return True


logging_config = {
    "version": 1,
    "disable_existing_loggers": False,
    "filters": {
        "add_hostname": {
            "()": HostnameFilter,
        },
    },
    "formatters": {
        "detailed": {
            "format": "%(asctime)s [%(hostname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "detailed",
            "filters": ["add_hostname"],
        },
    },
    "loggers": {
        "distributed": {
            "handlers": ["console"],
            "level": "INFO",
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "INFO",
    },
}

logging.config.dictConfig(logging_config)
