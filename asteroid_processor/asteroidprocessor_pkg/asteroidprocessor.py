import time
from asteroidprocessor_pkg.actions_pkg.create_asteroid import create_asteroid
from asteroidprocessor_pkg.actions_pkg.get_asteroid import get_asteroid_info
from asteroidprocessor_pkg.actions_pkg.update_asteroid import update_asteroid_info
from asteroidprocessor_pkg.actions_pkg.delete_asteroid import delete_asteroid_info
from asteroidprocessor_pkg.dumpreport_pkg.dumpreport import extract_data_dump_to
from commons_pkg.commons import extract
from redis_pkg.redis_library import read_data_from_stream, get_data


class AsteroidProcessor:
    def __init__(self, rconn, service_name, streamname, logger):
        self._rconn = rconn
        self._strm_name = streamname
        self._count = 10
        self._block_for_ms = 2000
        self._reportfile_name = 'asteroidsdetails.csv'
        self._instance_counter = 0
        self._generate_report_after_counter_value = 2
        self._report_counter = 2
        self._start = 1
        self._logger = logger
        self._service_name = service_name
        self._actions = {
            'post': create_asteroid,
            'get': get_asteroid_info,
            'put': update_asteroid_info,
            'delete': delete_asteroid_info
        }

    @property
    def rconn(self):
        return self._rconn

    @property
    def service_name(self):
        return self._service_name

    @property
    def reportfile_name(self):
        return self._reportfile_name

    @property
    def count(self):
        return self._count

    @property
    def blockfor_ms(self):
        return self._block_for_ms

    @property
    def instance_counter(self):
        return self._instance_counter

    @instance_counter.setter
    def instance_counter(self, value):
        self._instance_counter = value

    @property
    def logger(self):
        return self._logger

    def read_data_from_stream(self):
        data = read_data_from_stream(
            rconn=self._rconn, stream=self._strm_name, count=self._count, block=self._block_for_ms
        )
        for msg in data:
            extracted_data = extract(msg[1])
            self._logger.log(
                level='INFO', message=f'Incoming Data received at service {self._service_name}: {extracted_data}',
                req_id=None
            )
            method = extracted_data['method'].lower()
            # call the appropriate CRUD handler
            self._actions.get(method)(self, extracted_data)

    def check_and_dump_to_file(self):
        if self.instance_counter >= self._generate_report_after_counter_value:
            data = []
            for i in range(self._start, self._generate_report_after_counter_value):
                data.append(get_data(self._rconn, i).decode())

            if data:
                filename = f'{self.reportfile_name}_{int(time.time())}'
                self._logger.log(level='INFO', message=f'BEGIN:: Dumping data to {filename} ::BEGIN', req_id=None)
                extract_data_dump_to(filename, self, data)
                self._instance_counter = self._generate_report_after_counter_value
                self._start = self._instance_counter + 1
                self._generate_report_after_counter_value *= 2
                self._logger.log(level='INFO', message=f'END:: Dumping data to {filename} ::END', req_id=None)

