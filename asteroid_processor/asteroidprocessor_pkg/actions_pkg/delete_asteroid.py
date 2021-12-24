import json
from redis_pkg.redis_library import add_data_to_stream, delete_data


def delete_asteroid_info(obj_asteroid_proc, data):
    """
    Deletes a particular asteroid info with the (given)asteroid id
    """
    # print('data: ', data)
    req_id = data['asteroid_id']
    deleted = delete_data(obj_asteroid_proc.rconn, req_id)
    obj_asteroid_proc.logger.log(
        level='INFO', message=f'Received data in {delete_asteroid_info.__name__}: {data}', req_id=req_id
    )
    if deleted:
        ret_data = {
            'message': 'Asteroid Deleted',
            'asteroidId': req_id,
            'status': 'success',
            'response_code': 200
        }
        obj_asteroid_proc.logger.log(
            level='INFO', message=f'Asteroid deleted successfully: {ret_data}', req_id=req_id, type='response'
        )
    else:
        ret_data = {
            'error': 'Error message described below',
            'message': 'Asteroid Not Found',
            'response_code': 404,
            'status': 'failed'
        }
        obj_asteroid_proc.logger.log(
            level='ERROR', message=f'Asteroid deletion failed: {ret_data}', req_id=req_id, type='response'
        )
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )

