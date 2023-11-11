import os
import logging
import sys


class CustomAppLogger:
    @staticmethod
    def setup_logger():
        os.environ['LOG_DIRS'] = "C:\\Users\\Vishnu59\\PycharmProjects\\Pyspark_cdcframework"
        #print(os.environ)
        if not 'LOG_DIRS' in os.environ:
            sys.stderr.write('Missing LOG_DIRS environment variable, pyspark logging disabled')
            return

        file = os.environ['LOG_DIRS'].split(',')[0] + '/pyspark.log'
        logging.basicConfig(filename=file, level=logging.INFO,
                            format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s')

    def __getattr__(self, key):
        return getattr(logging, key)


CustomAppLogger.setup_logger()