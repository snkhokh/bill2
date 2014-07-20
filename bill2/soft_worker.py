__author__ = 'sn'
# coding=utf-8

from threading import Thread, Lock, Timer
from Queue import Queue, Empty
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection
import datetime

from config import dbhost, dbuser, dbpass, dbname, hosts_billing_proc_period, hosts_sessions_update_period
from commands import Command
from util.helpers import getLogger
from bill2.user import Users
from bill2.host import Hosts

logSys = getLogger(__name__)


class SoftWorker(Thread):
    def __init__(self):
        super(SoftWorker, self).__init__()
        self.__users = Users()
        self.__hosts = Hosts(self.__users)
        self.__db = None

    @property
    def db(self):
        return self.__db if isinstance(self.__db,Connection) else Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
####################################################

    def do_exit(self, cmd):
        isinstance(cmd, Command)
        logSys.debug('Stop cmd received!!!')
        self.__exit_flag = True
#####################################################

    cmd_router = {'stop': do_exit,
              'do_billing': do_billing,
              'update_sessions': update_sessions}


    def run(self):
        logSys.debug('Core billing started...')
        self.queue_update_sessions()
        Timer(hosts_billing_proc_period, self.queue_do_billing).start()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in self.cmd_router:
                    logSys.debug('Cmd: %s received', cmd.cmd)
                    self.cmd_router[cmd.cmd](self, cmd.params)
            except Empty:
                pass
        logSys.debug('Host handler done!!!')
#####################################################

    def put_cmd(self, cmd):
        self.__comq.put(cmd)


