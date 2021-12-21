import csv
import json
from redis_pkg.redis_library import add_data_to_stream


def get_asteroid_info(obj_asteroid_proc, data):
    found = False
    req_id = str()
    ret_data = dict()
    with open(obj_asteroid_proc.dbfile_name, mode='r') as asteroid_file_read:
        reader = csv.DictReader(asteroid_file_read)
        # print('data: ', data)
        for row in reader:
            req_id = row['request_id']
            if req_id == data['asteroid_id']:
                found = True
                ret_data = row
                break
    if found:
        del ret_data['method']
        del ret_data['response_stream_name']
        ret_data.update(response_code=200)
    else:
        ret_data = {
            'error': 'Error message described below',
            'message': 'Asteroid Not Found',
            'response_code': 404
        }
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
