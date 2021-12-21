
def add_data_to_stream(rconn, stream, data):
    rconn.xadd(name=stream, fields=data)


def read_data_from_stream(rconn, stream, count, block):
    return rconn.xread({stream: '$'}, count, block)
