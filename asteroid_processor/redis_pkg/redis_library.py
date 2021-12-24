
def add_data_to_stream(rconn, stream, data):
    """
    Adds the `data` to the (supplied)redis `stream` with the help of redis connection handler
    """
    rconn.xadd(name=stream, fields=data)


def read_data_from_stream(rconn, stream, count, block):
    """
    reads the data from the (supplied)redis `stream` with the help of redis connection handler. The amount of data read
    is dependent on the value of `count` and the `stream` for `block` milliseconds before a data is read.
    Note: Always a new data is read since the ID is taken as '$'.
    """
    return rconn.xread({stream: '$'}, count, block)


def set_data(rconn, key, value, config='info:asteroid'):
    """
    Sets the data in redis hash. In python it is read as config[key] = value
    """
    rconn.hset(config, key, value)


def get_data(rconn, key, config='info:asteroid'):
    """
    Gets the data from the redis hash
    """
    return rconn.hget(config, key)


def delete_data(rconn, key, config='info:asteroid'):
    """
    Deletes the key from the redis hash
    """
    return rconn.hdel(config, key)