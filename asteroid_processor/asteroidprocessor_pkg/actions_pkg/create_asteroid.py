import json
from redis_pkg.redis_library import add_data_to_stream, set_data
from asteroidprocessor_pkg.alerts_pkg.notifications import send_alert
from commons_pkg.commons import check_alert_params, validate_data


def create_asteroid(obj_asteroid_proc, data):
    """
    Validates the `data`, creates the asteroid and sends alert if info received over-shooted `alert_params`. Returns the
    response back to the calling service.
    """
    # print(type(data), data)
    req_id = data.get('request_id')
    obj_asteroid_proc.logger.log(
        level='INFO', message=f'Received data in {create_asteroid.__name__}: {data}', req_id=req_id
    )
    try:
        name = data.get('name', None)
        data.update(name=f'asteroids_{req_id}') if not name else None
        # validate incoming data
        ret = validate_data(data)
        if not ret:
            # if data validated then check for other parameters and send alert
            if check_alert_params(data):
                send_alert(data, obj_asteroid_proc.logger, req_id)
            set_data(obj_asteroid_proc.rconn, req_id, json.dumps(data))
            ret_data = {
                'message': 'Asteroid Created',
                'asteroidId': req_id,
                'status': 'success',
                'name': data.get('name'),
                'response_code': 201
            }

            obj_asteroid_proc.instance_counter += 1
            set_data(obj_asteroid_proc.rconn, obj_asteroid_proc.instance_counter, req_id)
            obj_asteroid_proc.logger.log(
                level='INFO', message=f'Asteroid created successfully with: {ret_data}', req_id=req_id, type='response'
            )
        else:
            ret_data = {
                'error': 'Error message described below',
                'message': ret,
                'response_code': 400,
                'status': 'failed'
            }
            obj_asteroid_proc.logger.log(
                level='ERROR', message=f'Asteroid creation failed due to: {ret_data}', req_id=req_id, type='response'
            )
    except Exception as ex:
        ret_data = {
            'error': 'Error message described below',
            'message': ex.__str__(),
            'response_code': 500,
            'status': 'failed'
        }
        obj_asteroid_proc.logger.log(
            level='CRITICAL', message=f'Asteroid creation failed due to: {ret_data}', req_id=req_id, type='response'
        )
    add_data_to_stream(
        rconn=obj_asteroid_proc.rconn, stream=data['response_stream_name'], data={req_id: json.dumps(ret_data)}
    )
