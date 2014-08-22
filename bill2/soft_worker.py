import datetime

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
from bill2.trafplan import TrafPlans

logSys = getLogger(__name__)


class SoftWorker(Thread):
    def __init__(self):
        super(SoftWorker, self).__init__()
        self.__tps = TrafPlans()
        self.__users = Users(self.__tps)
        self.__hosts = Hosts(self.__users)
        self.__db = None
        #: :type: TnetServer
        self.__tnserv = None
        self.__comq = Queue()
        self.__lock = Lock()
        self.__exit_flag = False
    ####################################################

    @property
    def db(self):
        return self.__db if isinstance(self.__db,Connection)\
            else Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
    ####################################################

    @property
    def hosts(self):
        return self.__hosts
    ####################################################

    @property
    def users(self):
        return self.__users
    ####################################################

    def fget_tps(self, mask):
        return self.__tps.fget(mask)
    ####################################################

    def fget_users(self, mask):
        if mask:
            return self.__users.fget(mask) + self.__hosts.fget(mask)
        else:
            return self.__users.fget(mask)
    ####################################################

    def fget_hosts(self, mask):
        return self.__hosts.fget(mask)
    ####################################################

    def set_tnserver(self, serv):
        self.__tnserv = serv
    ####################################################

    def prepare_state(self):
        with self.__lock:
            return self.__hosts.state_for_nas

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
        self.__exit_flag = True
    #####################################################

    def sessions_update(self, cmd=None):
        with self.__lock:
            self.hosts.sessions_update(self.db)
        #
        Timer(sessions_update_period, self.queue_update_sessions).start()
    ####################################################

    def do_billing(self, cmd=None):
        with self.__lock:
            now = datetime.datetime.now()
            stat_cnt = dict()
            upd_users = set()
            for host in (self.hosts[host_id] for host_id in self.hosts
                         if self.hosts[host_id].counter[0]+self.hosts[host_id].counter[1]):
                c = host.counter
                stat_cnt[host.db_id] = c
                host.counter_reset()
                if host.user.tp.have_limit:
                    if host.user.tp.daily_proc(now):
                        upd_users.add(host.user.db_id)
                    if host.user.tp.calc_traf(c, now):
                        upd_users.add(host.user.db_id)
            cur = self.db.cursor()
            if stat_cnt:
                for host_id in stat_cnt:
                    host = self.hosts[host_id]
                    cur.execute('INSERT INTO stat (user_id, host_id, ts, dw, up) VALUES (%s, %s, %s, %s, %s)',
                                (host.user.db_id, host.db_id, now) + stat_cnt[host_id])
            for user_id in upd_users:
                self.users[user_id].db_upd_tp_data(cur)
    #
        if __name__ != '__main__':
            Timer(billing_process_period, self.queue_do_billing).start()
    ####################################################

    def update_conf(self, cmd=None):
        with self.__lock:
            self.users.update(self.db)
            self.hosts.update(self.db)
        Timer(soft_config_update_period, self.queue_update_conf).start()
    ####################################################

    def reload_config(self):
        with self.__lock:
            self.load_from_db()
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
            self.load_from_db()
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
                  'update_sessions': sessions_update}
    ####################################################

    def load_from_db(self):
        self.__tps.load_all_tps(self.db)
        self.__users.load_all_users(self.db)
        self.__hosts.load_all_hosts(self.db)
    ####################################################

    def reload_tps(self):
        with self.__lock:
            self.__tps.load_all_tps(self.db)

####################################################

if __name__ == "__main__":
    import time
    dbname = 'traf_test'
    sw = SoftWorker()
    sw.load_from_db()
    print sw.fget_hosts('')
    while 1:
        sw.users.update(sw.db)
        sw.hosts.update(sw.db)
        sw.hosts.sessions_update(sw.db)
        sw.do_billing()
        print sw.prepare_state()
        time.sleep(5)
    exit()

from bill2.tserv import TnetServer
####################################################

