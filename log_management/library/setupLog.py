import logging
from logging.handlers import RotatingFileHandler

def setupLog(fileName, logLevel, maxBytes, backupCount):
    logHandler = RotatingFileHandler(fileName, mode='a', maxBytes=maxBytes, backupCount=backupCount)
    formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    logHandler.setFormatter(formatter)
    logger = logging.getLogger()
    logger.addHandler(logHandler)
    logger.setLevel(logLevel)
