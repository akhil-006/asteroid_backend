
def add_data_to_stream(rconn, stream, data):
    rconn.xadd(name=stream, fields=data)


def read_data_from_stream(rconn, stream, count, block):
    return rconn.xread({stream: '$'}, count, block)


def set_data(rconn, key, value, config='config:asteroid'):
    rconn.hset(config, key, value)


def get_data(rconn, key, config='config:asteroid'):
    return rconn.hget(config, key)


def delete_data(rconn, key, config='config:asteroid'):
    return rconn.hdel(config, key)