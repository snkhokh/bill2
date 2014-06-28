# coding=utf-8
__author__ = 'sn'

from threading import Thread, Lock, Timer
from Queue import Queue, Empty
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection

from config import dbhost, dbuser, dbpass, dbname, periodic_proc_timeout
from user import Users, User
from commands import Command
from util import net


class Host(object):
    def __init__(self, host_ip, lprefix, version, pool_id):
        self.__ip = host_ip
        self.__lprefix = lprefix
        self.__pool_id = pool_id
        self.__user = None
        self.__version = version
        self.__count_in = 0
        self.__count_out = 0
#
        self.static = False
        self.need_statistic = False
        self.is_ppp = False

    @property
    def ip_n(self):
        return self.__ip,self.__lprefix

    @property
    def ip_s(self):
        return net.ip_ntos(self.__ip,self.__lprefix)

    @property
    def ver(self):
        return self.__version

    @ver.setter
    def ver(self,n):
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
        return {'dw': self.__count_in, ' up': self.__count_out}

    @counter.setter
    def counter(self, c={'dw': 0, 'up': 0}):
        self.__count_in = c['dw']
        self.__count_out = c['up']

    def counter_update(self, dw=0, up=0):
        self.__count_in += dw
        self.__count_out += up


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

    def load_all_hosts(self):
        c = self.__db.cursor()
        assert isinstance(c, Cursor)

        # ToDo в базе д.б. признак привязки статика к конкретному НАС
        # загрузим инфу о сессиях
        c.execute('LOCK TABLES ip_sessions READ')
 #
        sql = 'SELECT max(unix_timestamp(start_time)) AS session_ver FROM ip_sessions WHERE stop_time IS NULL'
        c.execute(sql)
        self.__sessions_ver = c.fetchone()[0]
#
        sql = 'SELECT ip_pool_id,inet_aton(framed_ip) AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) as version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = net.ip_ntos(h['host_ip'])
#                host_id = str(h['ip_pool_id']) + '_' + str(h['host_ip'])
                host = None
                if not host_id in self.__hosts:
                    host = Host(h['host_ip'], 32, h['version'], h['ip_pool_id'])
                    self.__hosts[host_id] = host
                else:
                    host = self.__hosts[host_id]
                if h['version'] >= host.ver:
                    host.is_ppp = True
                    host.static = not h['ip_pool_id']
                    host.need_statistic = False
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                    host.user = self.__users.get_user(h['acc_uid'])
#
        c.execute('UNLOCK TABLES')
        c.execute('LOCK TABLES hostip READ')
#
        sql = 'SELECT max(version) AS hosts_ver FROM hostip WHERE dynamic =0'
        c.execute(sql)
        self.__hosts_ver = c.fetchone()[0]
#
        sql = 'SELECT int_ip,mask,PersonId,version FROM hostip WHERE dynamic = 0'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = net.ip_ntos(h['int_ip'],h['mask'])
                if not host_id in self.__hosts:
                    host = Host(h['int_ip'], h['mask'], h['version'], 0)
                    self.__hosts[host_id] = host
                    host.user = self.__users.get_user(h['PersonId'])
                    host.static = True
                    host.need_statistic = True
                else:
                    self.__hosts[host_id].ver = h['version']
        c.execute('UNLOCK TABLES')

    @property
    def db(self):
        return self.__db

    def get_host(self, host_id):
        with self.__lock:
            if host_id in self.__hosts:
                return dict(zip(('nas', 'host'), self.__hosts[host_id]))
            else:
                return None

    def get_hosts_needs_stat(self):
          with self.__lock:
            return (h for h in self.__hosts.keys() if self.__hosts[h].need_statistic)

    def get_reg_hosts(self):
          with self.__lock:
            return (h for h in self.__hosts.keys())

    def update_sessions(self):
        c = self.__db.cursor()
        assert isinstance(c, Cursor)

        c.execute('LOCK TABLES ip_sessions READ')

        sql = 'SELECT ip_pool_id,inet_aton(framed_ip) AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) as version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)

        with self.__lock:
            sessions = {self.__hosts[item].ip_n[0]: (self.__hosts[item].ver, item) for item in self.__hosts if self.__hosts[item].is_ppp}
            removed_hosts = set()
            added_hosts = set()
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                item = sessions.pop(h['host_ip'])
                if item:
                    if item[0] == h['version']:
                        continue
                    else:
                        removed_hosts.add(item[1])
                        self.__hosts.pop(item[1])


                host_id = str(h['ip_pool_id']) + '_' + str(h['host_ip'])
                if host_id in self.__hosts:
                    removed_hosts.add(item[1])

                added_hosts.add(host_id)




                host_id = str(h['ip_pool_id']) + '_' + str(h['host_ip'])
                host = None
                if not host_id in self.__hosts:
                    host = Host(h['host_ip'], 32, h['version'], 0)
                    self.__hosts[host_id] = host
                else:
                    host = self.__hosts[host_id]
                if h['version'] >= host.ver:
                    host.is_ppp = True
                    host.static = not h['ip_pool_id']
                    host.need_statistic = False
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                    host.user = self.__users.get_user(h['acc_uid'])
#
        c.execute('UNLOCK TABLES')




    def do_exit(self, cmd):
        isinstance(cmd, Command)
        print 'Stop cmd received!!!'
        self.__exit_flag = True

    def __do_timer(self, cmd):
        self.periodic_proc()
        self.__timer_handler(True)

    cmd_router = {'stop': do_exit,
                  'timer': __do_timer}

    def run(self):
        print 'Host handler started...'
        self.__timer_handler(True)
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in self.cmd_router:
                    self.cmd_router[cmd.cmd](self, cmd.params)
            except Empty:
                pass
        print 'Host handler done!!!'

    def put_cmd(self, cmd):
        self.__comq.put(cmd)

    def periodic_proc(self):
        print "Periodic procedure!!!"

    def __timer_handler(self, i=None):
        if i:
            Timer(periodic_proc_timeout, self.__timer_handler).start()
        else:
            self.put_cmd(Command('timer'))


