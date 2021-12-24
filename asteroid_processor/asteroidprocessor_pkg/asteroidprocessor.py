import time
from asteroidprocessor_pkg.actions_pkg.create_asteroid import create_asteroid
from asteroidprocessor_pkg.actions_pkg.get_asteroid import get_asteroid_info
from asteroidprocessor_pkg.actions_pkg.update_asteroid import update_asteroid_info
from asteroidprocessor_pkg.actions_pkg.delete_asteroid import delete_asteroid_info
from asteroidprocessor_pkg.dumpreport_pkg.dumpreport import extract_data_dump_to
from commons_pkg.commons import extract
from redis_pkg.redis_library import read_data_from_stream, get_data


class AsteroidProcessor:
    """
    Asteroid backend service which extracts the incoming data and processes it according to the HTTP method type.
    Also sends alert to a development mail and dumps the (20 recent) data in a (CSV) file.
    """
    def __init__(self, rconn, service_name, streamname, logger):
        self._rconn = rconn
        self._strm_name = streamname
        self._count = 10
        self._block_for_ms = 2000
        self._reportfile_name = 'asteroidsdetails.csv'
        self._instance_counter = 0
        self._generate_report_after_counter_value = 21
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
        """
        Get redis connection property
        """
        return self._rconn

    @property
    def service_name(self):
        """
        Get service name property
        """
        return self._service_name

    @property
    def reportfile_name(self):
        """
        Get (dumped)report file name property
        """
        return self._reportfile_name

    @property
    def count(self):
        """
        Get number of messages to be read from the stream property
        """
        return self._count

    @property
    def blockfor_ms(self):
        """
        Get the amount of time the stream has to wait for reading a message property
        """
        return self._block_for_ms

    @property
    def instance_counter(self):
        """
        Get how many asteroids have been created(stored) in redis property
        """
        return self._instance_counter

    @instance_counter.setter
    def instance_counter(self, value):
        """
        Set the number of (created)asteroids property
        """
        self._instance_counter = value

    @property
    def logger(self):
        """
        Get the logger instance for logging purposes
        """
        return self._logger

    def read_data_from_stream(self):
        """
        Reads the messages from the service's stream and handles it for further processing. Calls the appropriate
        HTTP method handler based on `self._actions`
        """
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
        """
        Checks whether the `self._instance_counter` is greater than `self.__generate_report_after_counter_value` and
        if found True then dumps the `self._generate_report_after_counter_value` (20 recent asteroids) in the csv file.
        """
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

