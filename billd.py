__author__ = 'sn'

import sys
from time import sleep
from bill2.nas_mikrotik import MikroNas
from bill2.user import Users
from bill2.host import Hosts
from bill2.commands import Command
from bill2.config import nases

def main():
    print 'Init start ...'
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
