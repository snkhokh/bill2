# coding=utf-8
__author__ = 'sn'

from threading import Thread,Lock,Timer
from Queue import Queue,Empty
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection

from config import dbhost, dbuser, dbpass, dbname, periodic_proc_timeout
from user import Users
from commands import Command


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

    @property
    def db(self):
        return self.__db

    def load_all_hosts(self):
        c = self.__db.cursor()
        assert isinstance(c,Cursor)

        # ToDo в базе д.б. признак привязки статика к конкретному НАС
        # загрузим инфу о сессиях

        sql = 'select * FROM ip_sessions WHERE stop_time is null and l_update > date_sub(now(),interval 6 minute)'
        c.execute(sql)

        c.execute('SELECT int_ip FROM hostip WHERE dynamic = 0')
        for row in c.fetchall():
            self.get_host('0_' + str(row[0]))


    def get_host(self, host_id):
        with self.__lock:
            if host_id in self.__hosts:
                return dict(zip(('nas', 'host'), self.__hosts[host_id]))
            else:
                return None

    def create_host(self, host_id, nas):
        with self.__lock:
            if host_id in self.__hosts:
                (tnas, thost) = self.__hosts[host_id]
                if id(nas) != id(tnas):
                    raise RuntimeError
                else:
                    return thost
            thost = Host(host_id, self.__db)
            self.__hosts[host_id] = (nas, thost)
            return thost

    def remove_host(self, host_id):
        with self.__lock:
            del self.__hosts[host_id]

    def do_exit(self, cmd):
        isinstance(cmd,Command)
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

    def putCmd(self, cmd):
        self.__comq.put(cmd)

    def periodic_proc(self):
        print "Periodic procedure!!!"

    def __timer_handler(self, i=None):
        if i:
            Timer(periodic_proc_timeout, self.__timer_handler).start()
        else:
            self.putCmd(Command('timer'))



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


class Host(object):
    def __init__(self, host_id, db):
        """

        :rtype : Host
        """
        (pool, ip) = host_id.split('_')
        self.ip = int(ip)
        self.pool = int(pool)
        if not self.pool:
            info = get_stat_host_info(ip, db)
        else:
            info = get_dyn_host_info(ip, pool, db)
        if not info:
            raise
        self.lprefix = info['lprefix']
        self.uid = info['uid']
        #a = getCurPolicyForUser(self.uid)
        #self.curSpeedDw,
        #self.curSpeedUp,
        #self.curFilterId) = info
        self.count_in = 0
        self.count_out = 0

    def test(self):
        pass
