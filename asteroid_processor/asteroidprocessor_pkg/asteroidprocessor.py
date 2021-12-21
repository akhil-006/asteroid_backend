from asteroidprocessor_pkg.actions_pkg.create_asteroid import create_asteroid
from asteroidprocessor_pkg.actions_pkg.get_asteroid import get_asteroid_info
from asteroidprocessor_pkg.actions_pkg.update_asteroid import update_asteroid_info
from asteroidprocessor_pkg.actions_pkg.delete_asteroid import delete_asteroid_info
from commons_pkg.commons import extract
from redis_pkg.redis_library import read_data_from_stream
from counter import  Counter



class AsteroidProcessor:
    instance_counter = Counter(1)
    def __init__(self, rconn, streamname):
        self._rconn = rconn
        self._strm_name = streamname
        self._count = 10
        self._block_for_ms = 2000
        self._dbfile_name = 'asteroidsdetails.csv'
        self._header = False
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
    def dbfile_name(self):
        return self._dbfile_name

    @property
    def count(self):
        return self._count

    @property
    def header(self):
        return self._header

    @header.setter
    def header(self, attrib):
        self._header = attrib

    @property
    def blockfor_ms(self):
        return self._block_for_ms

    def read_data_from_stream(self):
        data = read_data_from_stream(
            rconn=self._rconn, stream=self._strm_name, count=self._count, block=self._block_for_ms
        )
        for msg in data:
            extracted_data = extract(msg[1])
            method = extracted_data['method'].lower()
            # call the appropriate CRUD handler
            self._actions.get(method)(self, extracted_data)

    def check_and_dump_to_file(self):
        pass

