__author__ = 'sn'

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor
from threading import Lock
from Queue import Queue
from commands import Command
import json

from config import dbhost, dbuser, dbpass, dbname
from trafplan import TrafPlans, TP



class Users():
    def __init__(self):
        self.__users_lock = Lock()
        self.__comq = Queue()
        self.__db = Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, use_unicode=True, charset='cp1251')
        self.__tps = TrafPlans()
        self.__tps.load_all_tps(self.__db)
        self.__users = dict()
        self.load_all_users()

    def get_user(self, user_id):
        '''
        :param user_id:
        :rtype: User
        '''
        if user_id in self.__users:
            return self.__users[user_id]
        else:
            print 'Not found user for user_id: %s' % user_id
            return None

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
            self.__users[r['uid']] = User(r['uid'], r['name'], TP(self.__tps.get_tp(r['tp_id']), r['tp_data_json']))
        print 'Now %s users loaded...' % len(self.__users)


class User:
    def __init__(self, uid, name, tp):
        '''

        :param uid: user id in db
        :param name: User name
        :param tp: Traf plan
        :type tp: TP
        '''
        self.__uid = uid
        self.__name = name
        self.__tp = tp
        self.__version = 0

    @property
    def tp(self):
        return self.__tp

    @property
    def db_id(self):
        return self.__uid

    def db_upd_tp_data(self, c):
        '''
        :param c: db cursor
         :type c: Cursor
        '''
        c.execute('UPDATE persons SET Opt = %s WHERE id = %s', (self.tp.json_data, self.db_id))

