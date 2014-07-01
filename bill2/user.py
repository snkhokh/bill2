__author__ = 'sn'

from MySQLdb import connect
from threading import Thread, Lock
from Queue import Queue
from commands import Command
from config import dbhost, dbuser, dbpass, dbname
from trafplan import Traf_plans,TP



class Users():
    def __init__(self):
        self.__users_lock = Lock()
        self.__comq = Queue()
        self.__db = connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, use_unicode=True, charset='cp1251')
        self.__tps = Traf_plans()
        self.__tps.load_all_tps(self.__db)
        self.__users = dict()
        self.load_all_users()

    def get_user(self, user_id):
        if user_id in self.__users:
            return self.__users[user_id]
        else:
            print 'Not found user for user_id: %s' % user_id
            return None

    def appendUser(self, user):
        assert isinstance(user, User)
        with self.__users_lock:
            self.__users[user.uid] = user

    def putCmd(self, cmd):
        assert isinstance(cmd, Command)
        self.__comq.put(cmd)

    def run(self):
        while True:
            item = self.__comq.get()
            assert isinstance(item, Command)
            if item.uid in self.__users:
                print "Command for user id %s received - %s" % (item.uid, item.cmd)

    def load_all_users(self):
        cur = self.__db.cursor()
        cur.execute(
            'SELECT id AS uid,Name AS name,UnitRem AS cnt_dw, UnitRemOut AS cnt_up,TaxRateId AS tp_id,'
            'PrePayedUnits AS units FROM persons ')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            self.__users[row[0]] = User(r.pop('uid'),r.pop('name'),self.__tps.get_tp(r.pop('tp_id')),**r)
        print 'Now %s users loaded...' % len(self.__users)


class User:
    def __init__(self, uid, name, tp, **tp_arg):
        self.__uid = uid
        self.__name = name
        self.__tp = tp
        self.__tp_base = tp.make_param_for_user(**tp_arg)

    @property
    def uid(self):
        return self.__uid

    def isOn(self):
        if self.__units:
            return True
        return False

