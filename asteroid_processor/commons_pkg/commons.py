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
    ]
}


def getuniqueid():
    min = 000000000000000000000000
    max = 999999999999999999999999
    return str(random.randrange(start=min, stop=max))


def extract(data):
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
    everyasteroid_with = alert_params.get('everyAsteroidWith')
    alert_bruce_willis = False
    for key in alert_params.keys():
        if key in asteroid_info:
            if asteroid_info[key] >= alert_params[key]:
                alert_bruce_willis = True
                break

    if not alert_bruce_willis:
        for list_val in everyasteroid_with:
            keys = list(list_val.keys())
            for key in keys:
                if asteroid_info[key] >= list_val[key]:
                    alert_bruce_willis = True
                    break

    return alert_bruce_willis
