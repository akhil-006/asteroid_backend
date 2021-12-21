import json
from redis_pkg.redis_library import add_data_to_stream, get_data


def get_asteroid_info(obj_asteroid_proc, data):
    # print('data: ', data)
    req_id = data['asteroid_id']
    asteroid_info = get_data(obj_asteroid_proc.rconn, req_id)
    ret_data = json.loads(asteroid_info) if asteroid_info else None
    if ret_data:
        del ret_data['method']
        del ret_data['response_stream_name']
        ret_data.update(response_code=200)
    else:
        ret_data = {
            'error': 'Error message described below',
            'message': 'Asteroid Not Found',
            'response_code': 404,
            'status': 'failed'
        }
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
