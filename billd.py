#!/usr/bin/python
__author__ = 'sn'

from time import sleep
import signal

from bill2.nas_mikrotik import MikroNas
from bill2.user import Users
from bill2.host import Hosts
from bill2.commands import Command
from bill2.config import nases


def __sigTERMhandler(signum, frame):
    print ("Caught signal %d. Exiting" % signum)
    nas1.putCmd(Command('stop'))
    h.put_cmd(Command('stop'))
    quit()

def main():
    global h,nas1
    print 'Init start ...'
    signal.signal(signal.SIGTERM, __sigTERMhandler)
    signal.signal(signal.SIGINT, __sigTERMhandler)
    u = Users()
    h = Hosts(u)
    nas1 = MikroNas(hosts=h, address=nases['m1']['address'], login=nases['m1']['login'], passwd=nases['m1']['passwd'])
    print 'Init done!'
    nas1.start()
    h.start()
    sleep(600)
    nas1.putCmd(Command('stop'))
    h.put_cmd(Command('stop'))


if __name__ == '__main__':
    main()
