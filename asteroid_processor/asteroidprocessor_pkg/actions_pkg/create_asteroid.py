import json
import time
from redis_pkg.redis_library import add_data_to_stream, set_data


def create_asteroid(obj_asteroid_proc, data):
    # print(type(data), data)
    req_id = data.get('request_id')
    try:
        name = data.get('name', None)
        data.update(name=f'asteroids_{int(time.time())}') if not name else None
        set_data(obj_asteroid_proc.rconn, req_id, json.dumps(data))
        ret_data = {
            'message': 'Asteroid Created',
            'asteroidId': req_id,
            'status': 'success',
            'response_code': 201
        }
    except Exception as ex:
        ret_data = {
            'error': 'Error message described below',
            'message': ex.__str__(),
            'response_code': 500,
            'status': 'failed'
        }
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
