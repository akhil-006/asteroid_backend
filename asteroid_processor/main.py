from asteroidprocessor_pkg.asteroidprocessor import AsteroidProcessor
from redis_pkg.conn_handlers import connect
from logger_pkg.logger import Logger

# creating some global variables for easy implementation. Of course this won't be the way for production code.
stream_name = 'asteroids_stream'
service = 'asteroidprocessor'


def main():
    rconn = connect()
    objlog = Logger(rconn=rconn, service_name=service)
    obj_proc = AsteroidProcessor(rconn=rconn, streamname=stream_name, logger=objlog, service_name=service)

    while True:
        try:
            obj_proc.read_data_from_stream()
            obj_proc.check_and_dump_to_file()
        except Exception as ex:
            print('Exception: ', ex)


if __name__ == '__main__':
    main()
