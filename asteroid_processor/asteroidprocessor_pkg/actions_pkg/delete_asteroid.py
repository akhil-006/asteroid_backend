import json
from redis_pkg.redis_library import add_data_to_stream, delete_data


def delete_asteroid_info(obj_asteroid_proc, data):
    # print('data: ', data)
    req_id = data['asteroid_id']
    deleted = delete_data(obj_asteroid_proc.rconn, req_id)

    if deleted:
        ret_data = {
            'message': 'Asteroid Deleted',
            'asteroidId': req_id,
            'status': 'success',
            'response_code': 200
        }
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

