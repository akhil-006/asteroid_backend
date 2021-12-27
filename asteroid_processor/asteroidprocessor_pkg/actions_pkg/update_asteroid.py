import json
from commons_pkg.commons import fields, validate_data, check_alert_params
from redis_pkg.redis_library import add_data_to_stream, get_data, set_data
from asteroidprocessor_pkg.alerts_pkg.notifications import send_alert


def update_asteroid_info(obj_asteroid_proc, data):
    """
    Updates a particular asteroid info with the (given)asteroid id. Also checks if the incoming `data` is 'good' or not.
    If not good then sends appropriate response code with the details of anomalies. Secondly, if the data was "good"
    and it also surpasses the allowed limit of alert params then it also sends a notification/alert to the concerned
    with the asteroid details.
    """
    # logic for update asteroid
    req_id = data['asteroid_id']
    obj_asteroid_proc.logger.log(
        level='INFO', message=f'Received data in {update_asteroid_info.__name__}: {data}', req_id=req_id
    )
    asteroid_info = get_data(obj_asteroid_proc.rconn, req_id)
    ret_data = json.loads(asteroid_info) if asteroid_info else None
    if ret_data:
        err = validate_data(data)
        if not err:
            if check_alert_params(data):
                send_alert(data, obj_asteroid_proc.logger, req_id)

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
            obj_asteroid_proc.logger.log(
                level='INFO', message=f'Asteroid info updated successfully: {ret_data}', req_id=req_id, type='response'
            )
        else:
            ret_data = {
                'error': 'Error message described below',
                'message': err,
                'response_code': 400,
                'status': 'failed'
            }
            obj_asteroid_proc.logger.log(
                level='ERROR', message=f'Update Asteroid failed due to: {ret_data}', req_id=req_id, type='response'
            )

    else:
        ret_data = {
            'error': 'Error message described below',
            'message': 'Asteroid Not Found',
            'response_code': 404,
            'status': 'failed'
        }
        obj_asteroid_proc.logger.log(
            level='ERROR', message=f'Update Asteroid failed due to: {ret_data}', req_id=req_id, type='response'
        )

    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
