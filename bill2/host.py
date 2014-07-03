# coding=utf-8
__author__ = 'sn'

from threading import Thread, Lock, Timer
from Queue import Queue, Empty
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection
import datetime

from config import dbhost, dbuser, dbpass, dbname, hosts_billing_proc_period, hosts_sessions_update_period
from user import Users, User
from commands import Command
from util import net


class Host(object):
    def __init__(self, host_ip, lprefix):
# check ip and mask before create host
        m1 = 32 - lprefix
        ip = host_ip >> m1
        ip <<= m1
        assert ip == host_ip
#
        self.__ip = host_ip
        self.__lprefix = lprefix
        self.__pool_id = 0
        self.__user = None
        self.__version = 0
        self.__count_in = 0
        self.__count_out = 0
        self.__stat_count_in = 0
        self.__stat_count_out = 0
        self.__stat_is_set = False
        self.__db_id = 0
#
        self.is_ppp = False
        self.session_ver = 0

    @property
    def db_id(self):
        return self.__db_id

    @db_id.setter
    def db_id(self,db_id):
        self.__db_id = db_id

    @property
    def pool_id(self):
        return self.__pool_id

    @pool_id.setter
    def pool_id(self, n):
        self.__pool_id = n

    @property
    def ip_n(self):
        return self.__ip, self.__lprefix

    @property
    def ip_s(self):
        return net.ip_ntos(self.__ip, self.__lprefix)

    @property
    def ver(self):
        return self.__version

    @ver.setter
    def ver(self, n):
        self.__version = n

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, user_link):
        if not user_link is None:
            assert isinstance(user_link, User)
        self.__user = user_link

    @property
    def counter(self):
        return (self.__count_in, self.__count_out) if self.__stat_is_set else (0,0)

    @counter.setter
    def counter(self, c):
        if self.__stat_is_set:
            d_in = c['dw'] - self.__stat_count_in
            if d_in > 0:
                self.__count_in += d_in
            d_out = c['up'] - self.__stat_count_out
            if d_out > 0:
                self.__count_out += d_out
        else:
            self.__stat_is_set = True
        self.__stat_count_in = c['dw']
        self.__stat_count_out = c['up']

    def counter_reset(self):
        self.__count_in = 0
        self.__count_out = 0


class Hosts(Thread):
    def __init__(self, users):
        """

        :type users: Users
        """
        super(Hosts, self).__init__()
        self.__users = users
        self.__hosts_ver = 0
        self.__sessions_ver = 0
        self.__db = Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
        self.__exit_flag = False
        self.__comq = Queue()
        self.__lock = Lock()
        self.__hosts = dict()
        self.load_all_hosts()
        print 'Hosts created, info about %s hosts loaded...' % len(self.__hosts)
#####################################################

    @property
    def db(self):
        return self.__db
#####################################################

    def get_hosts_needs_stat(self):
        with self.__lock:
            return (h for h in self.__hosts.keys() if not self.__hosts[h].is_ppp)
#####################################################

    def load_all_hosts(self):
        c = self.__db.cursor()
        assert isinstance(c, Cursor)
        # загрузим инфу о сессиях
        sql = 'SELECT ip_pool_id,framed_ip AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) as version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = h['host_ip']
                if not host_id in self.__hosts:
                    host = Host(*net.ip_ston(h['host_ip']))
                    self.__hosts[host_id] = host
                    host.is_ppp = True
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                elif h['version'] >= self.__hosts[host_id].session_ver:
                    # новая сессия от уже обработанного IP
                    host = self.__hosts[host_id]
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                    host.counter_reset()
                host.user = self.__users.get_user(h['acc_uid'])
                host.pool_id = h['ip_pool_id']
                host.session_ver = h['version']
#
        try:
            # c.execute('LOCK TABLES hostip READ')
    #
            sql = 'SELECT max(version) AS hosts_ver FROM hostip WHERE dynamic =0'
            c.execute(sql)
            self.__hosts_ver = c.fetchone()[0]
    #
            sql = 'SELECT id, int_ip, mask, PersonId, version FROM hostip WHERE dynamic = 0'
            c.execute(sql)
            with self.__lock:
                for row in c.fetchall():
                    h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                    host_id = net.ip_ntos(h['int_ip'], h['mask'])
                    if not host_id in self.__hosts:
                        host = Host(h['int_ip'], h['mask'])
                        host.user = self.__users.get_user(h['PersonId'])
                        self.__hosts[host_id] = host
                    else:
                        host = self.__hosts[host_id]
                    host.ver = h['version']
                    host.db_id = h['id']
        finally:
            c.execute('UNLOCK TABLES')
#####################################################

    def update_sessions(self, cmd=None):
        c = self.db.cursor()
        assert isinstance(c, Cursor)
        sql = 'SELECT ip_pool_id,framed_ip AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) as version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            sessions = {item: self.__hosts[item].session_ver for item in self.__hosts.keys()
                        if self.__hosts[item].session_ver}
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = h['host_ip']
                old_ver = sessions.pop(host_id) if host_id in sessions else None
                if not host_id in self.__hosts:
                    host = Host(*net.ip_ston(h['host_ip']))
                    self.__hosts[host_id] = host
                else:
                    host = self.__hosts[host_id]
                if not old_ver or not old_ver == h['version']:
                    host.is_ppp = True
                    host.user = self.__users.get_user(h['acc_uid'])
                    host.session_ver = h['version']
                    host.pool_id = h['ip_pool_id']
                elif old_ver and not old_ver == h['version']:
                    # todo сбросить статистику в базу
                    host.counter = dict(dw=0, up=0)
                    host.counter_reset()
                host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
            for host_id in sessions.keys():
                # для этих хостов сессия звыершена
                self.__hosts[host_id].session_ver = 0
        #
        Timer(hosts_sessions_update_period, self.queue_update_sessions).start()
#####################################################

    def queue_update_sessions(self):
        self.put_cmd(Command('update_sessions'))
#####################################################

    def do_billing(self, cmd=None):
        now = datetime.datetime.now()
        stat_cnt = dict()
        with self.__lock:
            for host_id in self.__hosts:
                host = self.__hosts[host_id]
                c = host.counter
                if c[0] + c[1]:
                    if not host.user.db_id in stat_cnt:
                        stat_cnt[host.user.db_id] = {host.db_id : c}
                    elif not host.db_id in stat_cnt[host.user.db_id]:
                        stat_cnt[host.user.db_id][host.db_id] = c
                    else:
                        stat_cnt[host.user.db_id][host.db_id] =\
                            tuple(c[i[0]]+i[1] for i in enumerate(stat_cnt[host.user.db_id][host.db_id]))
                    host.counter_reset()
                    if host.user.tp.have_limit():
                        host.user.tp.calc_traf(c, now, host.user.tp_data)
        if stat_cnt:
            cur = self.db.cursor()
        for user_id in stat_cnt.keys():
            for host_id in stat_cnt[user_id].keys():
                cur.execute('INSERT INTO stat (user_id, host_id, ts, dw, up) VALUES (%s, %s, %s, %s, %s)',
                            (user_id, host_id, now) + stat_cnt[user_id][host_id])
        Timer(hosts_billing_proc_period, self.queue_do_billing).start()
#####################################################

    def queue_do_billing(self):
        self.put_cmd(Command('do_billing'))
#####################################################


    def update_stat_for_hosts(self,hosts):
        with self.__lock:
            for h, cnt in hosts:
                if h in self.__hosts and not self.__hosts[h].is_ppp:
                    self.__hosts[h].counter = cnt
#####################################################

    def get_reg_hosts(self):
        with self.__lock:
            return (h for h in self.__hosts.keys())
#####################################################

    def do_exit(self, cmd):
        isinstance(cmd, Command)
        print 'Stop cmd received!!!'
        self.__exit_flag = True
#####################################################

    cmd_router = {'stop': do_exit,
                  'do_billing': do_billing,
                  'update_sessions': update_sessions}

    def run(self):
        print 'Host handler started...'
        self.queue_update_sessions()
        Timer(hosts_billing_proc_period, self.queue_do_billing).start()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in self.cmd_router:
                    self.cmd_router[cmd.cmd](self, cmd.params)
            except Empty:
                pass
        print 'Host handler done!!!'
#####################################################

    def put_cmd(self, cmd):
        self.__comq.put(cmd)
#####################################################


