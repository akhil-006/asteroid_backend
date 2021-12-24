import time
from redis_pkg.redis_library import add_data_to_stream

log_level = [
    'INFO',
    'WARNING',
    'ERROR',
    'CRITICAL'
]


class Logger:
    """
    Logger helper class which logs the message and other necessary details in redis
    """
    def __init__(self, rconn, service_name, log_stream='logger:asteroid_log_stream'):
        self._service_name = service_name
        self._rconn = rconn
        self._log_stream = log_stream

    def log(self, level, message, req_id, type='request'):
        """
        Logs the necessary details for easy debugging and analysis purposes.
        """
        rid = req_id if req_id else 'None'
        log_msg = {
            'sender': self._service_name,
            'message': message,
            'timestamp': time.time(),
            'type': type,
            'request_id': rid,
            'level': level
        }
        add_data_to_stream(rconn=self._rconn, stream=self._log_stream, data=log_msg)
