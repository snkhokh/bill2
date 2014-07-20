__author__ = 'sn'

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor
from threading import Lock
from Queue import Queue
from commands import Command
from util.helpers import getLogger
from config import dbhost, dbuser, dbpass, dbname
from trafplan import TrafPlans, TP

logSys = getLogger(__name__)

class Users():
    def __init__(self):
        self.__users_lock = Lock()
        self.__version = 0
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
            logSys.error('Not found user for user_id: %s' % user_id)
            return None

    def load_all_users(self):
        cur = self.__db.cursor()
        cur.execute(
            'SELECT id AS uid, Name AS name, TaxRateId AS tp_id, Opt AS tp_data_json, version FROM persons WHERE NOT deleted')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            if self.__version < r['version']:
                self.__version = r['version']
            self.__users[r['uid']] = User(r['uid'], r['name'], TP(self.__tps.get_tp(r['tp_id']), r['tp_data_json']))
        logSys.debug('Now %s users loaded...',len(self.__users))


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

