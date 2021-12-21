from redis import Redis
from redis import exceptions


def connect(host='127.0.0.1', port=6379, pwd=None):
    rconn = Redis(host=host, port=port)
    retries, retry_conn_counter = 0, 3
    while not rconn:
        rconn = Redis(host=host, port=port)
        retries += 1
        # Trying to get the redis connection 3 times.
        if retries >= retry_conn_counter:
            raise exceptions.ConnectionError('Redis server connection error!')

    return rconn
