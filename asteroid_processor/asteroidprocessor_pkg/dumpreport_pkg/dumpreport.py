import csv
import json
from commons_pkg.commons import fields
from redis_pkg.redis_library import get_data


def extract_data_dump_to(file, objproc, data):
    """
    Saves/dumps the recent 20 asteroid info in the (csv)`file`. The file is unique and is generated for every 20 recent
    asteroids. Thus each file will contain only 20 recent asteroid data.
    """
    with open(file, mode='w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields)
        writer.writeheader()
        for value in data:
            row = json.loads(get_data(objproc.rconn, value))
            row.pop('response_stream_name', None)
            row.pop('request_id', None)
            row.pop('method', None)
            writer.writerow(row)
        objproc.logger.log(level='INFO', message=f'Asteroid info dumped successfully', req_id=None, type='dump')
