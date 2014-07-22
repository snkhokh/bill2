__author__ = 'sn'
# coding=utf-8

from threading import Thread, Timer
from Queue import Queue, Empty
from MySQLdb.connections import Connection

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
        self.__comq = Queue()
        self.__exit_flag = False

    @property
    def db(self):
        return self.__db if isinstance(self.__db,Connection) else Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
    ####################################################

    def prepare_state(self):
        return self.__hosts.prepare_state()
    ####################################################

    def update_stat_for_hosts(self,hosts):
        self.__hosts.update_stat_for_hosts(hosts)
    ####################################################

    def get_hosts_needs_stat(self):
        return self.__hosts.get_hosts_needs_stat()
    ####################################################

    def do_exit(self, cmd):
        isinstance(cmd, Command)
        logSys.debug('Stop cmd received!!!')
        self.__exit_flag = True
    #####################################################

    def update_sessions(self, cmd = None):
        self.__hosts.update_sessions(self.db)
        #
        Timer(hosts_sessions_update_period, self.queue_update_sessions).start()
    ####################################################

    def do_billing(self, cmd = None):
        self.__hosts.do_billing(self.db)
        #
        Timer(hosts_billing_proc_period, self.queue_do_billing).start()
    ####################################################

    def run(self):
        logSys.debug('billing system core started...')
        self.__users.load_all_users(self.db)
        self.__hosts.load_all_hosts(self.db)
        self.queue_update_sessions()
        self.queue_do_billing()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in self.cmd_router:
                    logSys.debug('Cmd: %s received', cmd.cmd)
                    self.cmd_router[cmd.cmd](self, cmd.params)
            except Empty:
                pass
        logSys.debug('soft configuration handler done!')

    def put_cmd(self, cmd):
        self.__comq.put(cmd)
    #####################################################

    def queue_do_billing(self):
        self.put_cmd(Command('do_billing'))
    #####################################################

    def queue_update_sessions(self):
        self.put_cmd(Command('update_sessions'))
    ####################################################

    cmd_router = {'stop': do_exit,
                  'do_billing': do_billing,
                  'update_sessions': update_sessions}
####################################################
####################################################
