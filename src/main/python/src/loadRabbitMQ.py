import os
import sys
import time
import logging
import json
import pika

from ConfigParser import RawConfigParser

cfg = RawConfigParser()

def currentDayStr():
    return time.strftime("%Y%m%d")

def currentTimeStr():
    return time.strftime("%H:%M:%S")


def initLog(rightNow):
    logger = logging.getLogger(cfg.get('logging', 'logName'))
    logPath=cfg.get('logging', 'logPath')
    logFilename=cfg.get('logging', 'logFileName')  
    hdlr = logging.FileHandler(logPath+rightNow+logFilename)
    formatter = logging.Formatter(cfg.get('logging', 'logFormat'),cfg.get('logging', 'logTimeFormat'))
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr) 
    logger.setLevel(logging.INFO)
    return logger

def getCmdLineParser():
    import argparse
    desc = 'Execute rabbitMQLoader'
    parser = argparse.ArgumentParser(description=desc)

    parser.add_argument('-c', '--config_file', default='../config/rabbitMQLoader.conf',
                        help='configuration file name (*.ini format)')

    return parser




def main(argv):

    # Overhead to manage command line opts and config file
    p = getCmdLineParser()
    args = p.parse_args()

    cfg.read(args.config_file)

    # Get the logger going
    logger = initLog(time.strftime("%Y%m%d%H%M%S"))
    logger.info('Starting Run: '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')
    logger.info('   Processing file: '+cfg.get('data', 'source'))
    file = open(cfg.get('data', 'source'), 'r')


    credentials = pika.PlainCredentials(cfg.get('credentials', 'login'), cfg.get('credentials', 'password'))
    connection = pika.BlockingConnection(pika.ConnectionParameters(cfg.get('server', 'ip'),int(cfg.get('server', 'port')),'/',credentials))
    channel = connection.channel()
    count = 0
    for line in file:
        channel.basic_publish(cfg.get('server', 'exchange'),cfg.get('server', 'key'),body=line)
        count = count + 1
        if count % 1000 == 0:
            logger.info('   Processed line: '+str(count))
    connection.close

    # Clean up
    logger.info('   Total Lines Processed: '+str(count))
    logger.info('Done! '+time.strftime("%Y%m%d%H%M%S")+'  ==============================')

if __name__ == "__main__":
    main(sys.argv[1:])

