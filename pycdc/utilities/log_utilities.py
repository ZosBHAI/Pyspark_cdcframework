import os
import logging
import sys

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# Setting the executor log file location
handler = logging.FileHandler('C:\\Users\\Vishnu59\\PycharmProjects\\Pyspark_cdcframework\\pycdc\\executor.log','w+')

handler.setLevel(logging.DEBUG)

# Setting the log format
formatter = logging.Formatter('%(asctime)s -' ' %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
