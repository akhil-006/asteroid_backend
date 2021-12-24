import json
import random

# DO NOT change the sequence of this list. If you want to add another field/element then append it at last (after the
# comma)
fields = [
    'type', 'sizeMeters', 'distanceFromEarthAU', 'location', 'probabilityOfCollisionWithEarth',
    'timeOfObservation', 'name',
]


alert_params = {
    'sizeMeters': 1000,
    'probabilityOfCollisionWithEarth': 0.9,
    'everyAsteroidWith': [
        {
            'sizeMeters': 100,
            'probabilityOfCollisionWithEarth': 0.7
        },
        # add any other dictionary of alert parameters(just like above)
    ]
}


def getuniqueid():
    """
    Generates the unique id which is associated to a particular request and is also associated to a Asteroid(asteroid ID)
    """
    min = 000000000000000000000000
    max = 999999999999999999999999
    return str(random.randrange(start=min, stop=max))


def extract(data):
    """
    Extracts the incoming `data` from the front-end service
    """
    actual_data_dict = dict()
    i = 1
    for detail_list in data:
        actual_data_key = dict()
        actual_data_dictionary = detail_list[i]
        for subdetails_dict in actual_data_dictionary:
            actual_data_key = subdetails_dict.decode()
            actual_data_dict = json.loads(actual_data_dictionary[subdetails_dict].decode())
        actual_data_dict.update({'request_id': actual_data_key})
        i += 1

    return actual_data_dict


def check_alert_params(asteroid_info):
    """
    Checks the `asteroid_info` against the specified(/prescribed) `alert_params`.
    """
    everyasteroid_with = alert_params.get('everyAsteroidWith')
    alert_bruce_willis = False
    for key in alert_params.keys():
        if key in asteroid_info:
            if asteroid_info[key] >= alert_params[key]:
                alert_bruce_willis = True
                break

    if not alert_bruce_willis:
        check_list = []
        for list_val in everyasteroid_with:
            keys = list(list_val.keys())
            for key in keys:
                check_list.append(True) if asteroid_info[key] >= list_val[key] else check_list.append(False)

        if all(check_list) is True:
            alert_bruce_willis = True

    return alert_bruce_willis


def validate_data(info):
    """
    Validates the `info` i.e. checks whether the json-fields(supplied in the request body) are valid or not. If not
    valid, then notifies the caller(user) about anomalies.
    """
    keys = list(info.keys())
    del keys[keys.index('request_id')]
    del keys[keys.index('response_stream_name')]
    del keys[keys.index('method')]
    if keys != fields:
        unknown_fields = [field for field in keys if field not in fields]
        return f'Valid Fields are {fields}. Unknown fields supplied: {unknown_fields}'
    return str()
