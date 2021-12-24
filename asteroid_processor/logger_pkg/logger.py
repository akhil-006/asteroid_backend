import time

log_level = [
    'INFO',
    'WARNING',
    'ERROR',
    'CRITICAL'
]


class Logger:
    def __init__(self, rconn, service_name, log_stream='logger:asteroid_log_stream'):
        self._service_name = service_name
        self._rconn = rconn
        self._log_stream = log_stream

    def log(self, level, message, req_id, type='request'):
        rid = req_id if req_id else 'None'
        log_msg = {
            'sender': self._service_name,
            'message': message,
            'timestamp': time.time(),
            'type': type,
            'request_id': rid,
            'level': level
        }
        self._rconn.xadd(self._log_stream, log_msg)


