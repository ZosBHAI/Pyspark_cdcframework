import logging
import os

def setUpLogging(loggerName, level=logging.INFO) :
    logger = logging.getLogger(loggerName)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s : %(levelname)s : %(name)s : %(filename)s : %(lineno)d : %(message)s')
    formatter.default_msec_format = '%s.%03d'
    #formatter.default_msec_format = '%s'

    # Sets up file handler
    os.environ['LOG_DIRS'] = "C:\\Users\\Vishnu59\\PycharmProjects\\Pyspark_cdcframework"
    file = os.environ['LOG_DIRS'].split(',')[0] + '/cdc_app.log'
    fileHandler = logging.FileHandler(file)
    fileHandler.setFormatter(formatter)

    # Sets up handler to std out
    streamHandler = logging.StreamHandler()
    streamHandler.setFormatter(formatter)
    streamHandler.setLevel(logging.INFO)

    # Adds handler to logger
    logger.addHandler(fileHandler)
    logger.addHandler(streamHandler)
    return logger


