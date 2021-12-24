import json
from redis_pkg.redis_library import add_data_to_stream, get_data


def get_asteroid_info(obj_asteroid_proc, data):
    """
    Retrieves a particular asteroid info with the (given)asteroid id
    """
    # print('data: ', data)
    req_id = data['asteroid_id']
    obj_asteroid_proc.logger.log(
        level='INFO', message=f'Received data in {get_asteroid_info.__name__}: {data}', req_id=req_id
    )
    asteroid_info = get_data(obj_asteroid_proc.rconn, req_id)
    ret_data = json.loads(asteroid_info) if asteroid_info else None
    if ret_data:
        del ret_data['method']
        del ret_data['response_stream_name']
        ret_data.update(response_code=200)
        obj_asteroid_proc.logger.log(
            level='INFO', message=f'Asteroid info retrieved successfully: {ret_data}', req_id=req_id, type='response'
        )
    else:
        ret_data = {
            'error': 'Error message described below',
            'message': 'Asteroid Not Found',
            'response_code': 404,
            'status': 'failed'
        }
        obj_asteroid_proc.logger.log(
            level='ERROR', message=f'Retrieval of asteroid info failed: {ret_data}', req_id=req_id, type='response'
        )
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
