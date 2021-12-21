import csv
import json
from redis_pkg.redis_library import add_data_to_stream


def delete_asteroid_info(obj_asteroid_proc, data):
    # logic for delete asteroid
    req_id = data['asteroid_id']
    ret_data = {
        'message': 'Feature coming soon!',
        'asteroidId': req_id,
        'status': ':-)',
        'response_code': 200
    }
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
