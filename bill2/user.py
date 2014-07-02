__author__ = 'sn'

from MySQLdb import connect
from threading import Thread, Lock
from Queue import Queue
from commands import Command
import json

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
            'SELECT id AS uid, Name AS name, TaxRateId AS tp_id, Opt AS tp_data_json FROM persons')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            self.__users[row[0]] = User(r['uid'], r['name'], self.__tps.get_tp(r['tp_id']),
                                        tp_data_json=r['tp_data_json'])
        print 'Now %s users loaded...' % len(self.__users)


class User:
    def __init__(self, uid, name, tp, **tp_arg):
        self.__uid = uid
        self.__name = name
        self.__tp = tp
        self.__tp_base = tp.make_param_for_user(**tp_arg)
        self.__version = 0

    @property
    def tp(self):
        return self.__tp

    @property
    def tp_data(self):
        return self.__tp_base

    @property
    def uid(self):
        return self.__uid

    def update_db_data(self):
        pass

