import csv
import json
import time
from redis_pkg.redis_library import add_data_to_stream


def create_asteroid(obj_asteroid_proc, data):
    # print(type(data), data)
    try:
        req_id = str()
        with open(obj_asteroid_proc.dbfile_name, mode='a') as asteroids_file_write:
            req_id = data.get('request_id')
            name = data.get('name', None)
            data.update(name=f'asteroids_{int(time.time())}') if not name else None
            fieldnames = list(data.keys())
            writer = csv.DictWriter(asteroids_file_write, fieldnames=fieldnames)
            if not obj_asteroid_proc.header:
                writer.writeheader()
                obj_asteroid_proc.header = True
            writer.writerow(data)
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
            'response_code': 500
        }
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
