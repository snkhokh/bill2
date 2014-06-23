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
    def __init__(self, host_ip, lprefix):
        self.__ip = host_ip
        self.__lprefix = lprefix
        self.__user = None
        self.__is_ppp = False
        # a = getCurPolicyForUser(self.uid)
        #self.curSpeedDw,
        #self.curSpeedUp,
        #self.curFilterId) = info
        self.__count_in = 0
        self.__count_out = 0

    @property
    def is_ppp(self):
        return self.__is_ppp

    @is_ppp.setter
    def is_ppp(self, p):
        self.__is_ppp = p

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
        self.__lock = Lock()
        self.__hosts = dict()
        self.__db = Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
        self.__exit_flag = False
        self.__comq = Queue()
        self.load_all_hosts()

    @property
    def db(self):
        return self.__db

    def host(self, host_id):
        """
        :rtype : Host
        :param host_id:
        """
        return self.__hosts[host_id] if host_id in self.__hosts else None

    def load_all_hosts(self):
        c = self.__db.cursor()
        assert isinstance(c, Cursor)

        # ToDo в базе д.б. признак привязки статика к конкретному НАС
        # загрузим инфу о сессиях

        sql = 'SELECT ip_pool_id,inet_aton(framed_ip) AS host_ip,in_octets,out_octets,acc_uid' \
              ' FROM ip_sessions WHERE stop_time IS NULL AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = str(h['ip_pool_id']) + '_' + str(h['host_ip'])
                host = None
                if not host_id in self.__hosts:
                    host = Host(h['host_ip'], 32)
                    self.__hosts[host_id] = host
                else:
                    host = self.__hosts[host_id]
                host.is_ppp = True
                host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                host.user = self.__users.get_user(h['acc_uid'])

        sql = 'SELECT int_ip,mask,PersonId FROM hostip WHERE dynamic = 0'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = '0_' + str(h['int_ip'])
    #            host = None
                if not host_id in self.__hosts:
                    host = Host(h['int_ip'], h['mask'])
                    self.__hosts[host_id] = host
                    host.user = self.__users.get_user(h['PersonId'])
    #            else: host = self.__hosts[host_id]

        print 'Info about %s hosts loaded...' % len(self.__hosts)

    def get_host(self, host_id):
        with self.__lock:
            if host_id in self.__hosts:
                return dict(zip(('nas', 'host'), self.__hosts[host_id]))
            else:
                return None

    def remove_host(self, host_id):
        with self.__lock:
            del self.__hosts[host_id]

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


