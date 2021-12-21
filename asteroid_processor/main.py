from asteroidprocessor_pkg.asteroidprocessor import AsteroidProcessor
from redis_pkg.conn_handlers import connect

# creating some global variables for easy implementation. Of course this won't be the way for production code.
stream_name = 'asteroids_stream'


def main():
    rconn = connect()
    obj_proc = AsteroidProcessor(rconn=rconn, streamname=stream_name)

    while True:
        try:
            obj_proc.read_data_from_stream()
            obj_proc.check_and_dump_to_file()
        except Exception as ex:
            print('Exception: ', ex)


if __name__ == '__main__':
    main()
