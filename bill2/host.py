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
        # a = getCurPolicyForUser(self.uid)
        #self.curSpeedDw,
        #self.curSpeedUp,
        #self.curFilterId) = info
        self.__count_in = 0
        self.__count_out = 0

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, user_link):
        assert isinstance(user_link, User)
        self.__user = user_link

    @property
    def counter(self):
        return {'dw': self.__count_in, ' up': self.__count_out}

    @counter.setter
    def counter(self, c={'dw':0,'up':0}):
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
                if not host_id in self.__hosts:
                    self.__hosts[host_id] = Host(h['host_ip'], 32)
                self.host(host_id).counter = dict(dw=h['out_octets'], up=h['in_octets'])
                u = self.__users.get_user(h['acc_uid'])
                if u:
                    self.host(host_id).user = u


        # c.execute('SELECT int_ip FROM hostip WHERE dynamic = 0')
        # for row in c.fetchall():
        # self.get_host('0_' + str(row[0]))
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


def get_stat_host_info(ip, db):
    cur = db.cursor()
    assert isinstance(cur, Cursor)
    cur.execute('SELECT PersonId, mask, flags from hostip where int_ip = %s', (ip,))
    row = cur.fetchone()
    if not row:
        return None
    r = dict()
    r['uid'] = row[0]
    r['lprefix'] = row[1]
    r['flags'] = row[2]
    return r


def get_dyn_host_info(ip, pool, db):
    return False


