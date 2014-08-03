__author__ = 'sn'
# coding=utf-8

from threading import Thread, Timer, Lock
from Queue import Queue, Empty
from MySQLdb.connections import Connection

from config import sessions_update_period, dbhost, dbname, dbpass, billing_process_period, dbuser, \
    soft_config_update_period
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
        self.__lock = Lock()
        self.__exit_flag = False
    ####################################################

    @property
    def db(self):
        return self.__db if isinstance(self.__db,Connection) else Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
    ####################################################

    def prepare_state(self):
        with self.__lock:
            return self.__hosts.prepare_state()
    ####################################################

    def update_stat_for_hosts(self,hosts):
        with self.__lock:
            self.__hosts.update_stat_for_hosts(hosts)
    ####################################################

    def get_hosts_needs_stat(self):
        with self.__lock:
            return self.__hosts.get_hosts_needs_stat()
    ####################################################

    def do_exit(self, cmd):
        isinstance(cmd, Command)
        self.__exit_flag = True
    #####################################################

    def update_sessions(self, cmd = None):
        with self.__lock:
            self.__hosts.update_sessions(self.db)
        #
        Timer(sessions_update_period, self.queue_update_sessions).start()
    ####################################################

    def do_billing(self, cmd=None):
        with self.__lock:
            self.__hosts.do_billing(self.db)
        #
        Timer(billing_process_period, self.queue_do_billing).start()
    ####################################################

    def update_conf(self, cmd=None):
        with self.__lock:
            self.__users.update_users(self.db)
            self.__hosts.update_hosts(self.db)
        Timer(soft_config_update_period, self.queue_update_conf).start()
    ####################################################

    def run(self):
        logSys.debug('billing system core starting...')
        with self.__lock:
            c = self.db.cursor()
            try:
                c.execute('LOCK TABLES persons WRITE, hostip WRITE, traf_planes WRITE')
                c.execute('UPDATE persons SET version = 0')
                c.execute('UPDATE hostip SET version = 0')
                c.execute('UPDATE traf_planes SET version = 0')
            finally:
                c.execute('UNLOCK TABLES')
            self.__users.load_all_users(self.db)
            self.__hosts.load_all_hosts(self.db)
        self.queue_update_sessions()
        self.queue_do_billing()
        self.queue_update_conf()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in self.cmd_router:
                    # logSys.debug('Cmd: %s received', cmd.cmd)
                    self.cmd_router[cmd.cmd](self, cmd.params)
            except Empty:
                pass
        logSys.debug('soft configuration handler done!')
    #####################################################

    def put_cmd(self, cmd):
        self.__comq.put(cmd)
    #####################################################

    def queue_do_billing(self):
        self.put_cmd(Command('do_billing'))
    #####################################################

    def queue_update_sessions(self):
        self.put_cmd(Command('update_sessions'))
    ####################################################

    def queue_update_conf(self):
        self.put_cmd(Command('update_conf'))
    ####################################################

    cmd_router = {'stop': do_exit,
                  'do_billing': do_billing,
                  'update_conf': update_conf,
                  'update_sessions': update_sessions}
####################################################
####################################################
