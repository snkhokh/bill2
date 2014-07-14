__author__ = 'sn355_000'
import logging

def getLogger(name):
    if "." in name:
        name = "bill2.%s" % name.rpartition(".")[-1]
    return logging.getLogger(name)



