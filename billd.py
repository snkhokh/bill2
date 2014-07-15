#!/usr/bin/python

__author__ = 'sn'
import getopt, sys, os
from time import sleep

from bill2.server import Server
from bill2.version import version
from util.helpers import getLogger


# Gets the instance of the logger.
logSys = getLogger("bill2")


class Bill2Server:

    def __init__(self):
        self.__server = None
        self.__argv = None
        self.__conf = dict()
        self.__conf["background"] = False
        self.__conf["socket"] = "/var/run/bill2.sock"
        self.__conf["pidfile"] = "/var/run/bill2.pid"

    def dispVersion(self):
        print "Bill2 v %s" % version
        print "Many contributions by Cyril Jaquier <cyril.jaquier@fail2ban.org>."

    def disp_usage(self):
        print "Usage: "+self.__argv[0]+" [OPTIONS]"
        print
        print "Options:"
        print "    -b                   start in background"
        print "    -f                   start in foreground"
        print "    -s <FILE>            socket path"
        print "    -p <FILE>            pidfile path"
        print "    -h, --help           display this help message"
        print "    -V, --version        print the version"

    def __getCmdLineOptions(self, optList):
        """ Gets the command line options
        """
        for opt in optList:
            if opt[0] == "-b":
                self.__conf["background"] = True
            if opt[0] == "-f":
                self.__conf["background"] = False
            if opt[0] == "-s":
                self.__conf["socket"] = opt[1]
            if opt[0] == "-p":
                self.__conf["pidfile"] = opt[1]
            if opt[0] in ["-h", "--help"]:
                self.disp_usage()
                sys.exit(0)
            if opt[0] in ["-V", "--version"]:
                self.dispVersion()
                sys.exit(0)

    def start(self, argv):
        self.__argv = argv
        try:
            cmdOpts = 'bfs:p:hV'
            cmdLongOpts = ['help', 'version']
            optList, args = getopt.getopt(self.__argv[1:], cmdOpts, cmdLongOpts)
        except getopt.GetoptError:
            self.disp_usage()
            sys.exit(-1)

        self.__getCmdLineOptions(optList)

        try:
            self.__server = Server(self.__conf["background"])
            self.__server.start(self.__conf['socket'], self.__conf['pidfile'])
            return True
        except Exception, e:
            logSys.exception(e)
            self.__server.quit()
            return False

if __name__ == "__main__":
    server = Bill2Server()
    if server.start(sys.argv):
        sys.exit(0)
    else:
        sys.exit(-1)