import logging
import logging.config
import socket


class HostnameLogRecord(logging.LogRecord):
    def getMessage(self):
        msg = super().getMessage()
        return f"[{socket.gethostname()}] {msg}"


logging.setLogRecordFactory(HostnameLogRecord)

logging_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'stream': 'ext://sys.stdout',
        },
    },
    'loggers': {
        'distributed': {
            'handlers': ['console'],
            'level': 'INFO',
        },
    },
    'root': {
        'handlers': ['console'],
        'level': 'INFO',
    },
}

logging.config.dictConfig(logging_config)
