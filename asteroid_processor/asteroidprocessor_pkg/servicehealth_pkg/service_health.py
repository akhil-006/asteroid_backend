import json
from redis_pkg.redis_library import add_data_to_stream

health_status = ['good', 'poor']


def get_service_health(obj_asteroid_proc, data):
    """
    Pings the redis and based upon the ping status returns the response to the caller service. If the redis-ping
    returns with `False` then this service won't be able to respond to the caller service, in that situation, the
    caller service itself is capable of returning a useful message to the client/user.
    """
    req_id = data['request_id']
    ret_data = {
        'services': {
            data['service_name']: health_status[0]
        },
        'response_code': 200,
        'status': 'success'
    }
    obj_asteroid_proc.logger.log(
        level='INFO', message=f'{obj_asteroid_proc.service_name.upper()} health details: {ret_data}', req_id=req_id,
        type='response'
    )

    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )

