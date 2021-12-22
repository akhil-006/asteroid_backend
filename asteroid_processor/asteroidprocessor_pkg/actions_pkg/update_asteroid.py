import json
from commons_pkg.commons import fields
from redis_pkg.redis_library import add_data_to_stream, get_data, set_data


def update_asteroid_info(obj_asteroid_proc, data):
    # logic for delete asteroid
    req_id = data['asteroid_id']
    asteroid_info = get_data(obj_asteroid_proc.rconn, req_id)
    ret_data = json.loads(asteroid_info) if asteroid_info else None
    if ret_data:
        fields_to_be_updated = list(data.keys())
        for field in fields:
            if field in fields_to_be_updated:
                ret_data[field] = data[field]
        set_data(obj_asteroid_proc.rconn, req_id, json.dumps(ret_data))
        ret_data = {
            'message': 'Asteroid Info Updated',
            'name': ret_data.get('name'),
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
