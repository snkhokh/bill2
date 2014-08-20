import re

__author__ = 'sn'

from MySQLdb.connections import Connection
from MySQLdb.cursors import Cursor
from util.helpers import getLogger
from trafplan import TrafPlans, TP

logSys = getLogger(__name__)

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
    ####################################################

    @property
    def tp(self):
        return self.__tp
    ####################################################

    @tp.setter
    def tp(self, tp):
        assert isinstance(tp,TP)
        self.__tp = tp
    ####################################################

    @property
    def name(self):
        return self.__name
    ####################################################

    @name.setter
    def name(self, n):
        self.__name = n
    ####################################################

    @property
    def db_id(self):
        return self.__uid
    ####################################################

    def __str__(self):
        return 'uid=%s name=%s' % (self.__uid, self.__name)
    ####################################################

    def fget(self):
        return ('Uid: %s' % self.__uid, 'Name: %s' % self.__name) + self.tp.fget()
    ####################################################

    def db_upd_tp_data(self, c):
        ''' :type c: Cursor '''
        c.execute('UPDATE persons SET Opt = %s WHERE id = %s', (self.tp.json_data, self.db_id))
    ####################################################
    ####################################################


class Users():
    def __init__(self, trafplans):
        ''' :type trafplans: TrafPlans '''
        self.__tps = trafplans
        self.__version = 0
        self.__users = dict()
    ####################################################

    def __iter__(self):
        return self.__users.keys().__iter__()
    ####################################################

    def __getitem__(self, item):
        ''' :rtype: User '''
        if item in self.__users:
            return self.__users[item]
        else:
            logSys.error('Not found user for user_id: %s' % item)
            return None
    ####################################################

    def fget(self, mask):
        re_pat = re.compile(mask) if mask else None
        retl = ()
        for uid, u in self.__users.items():
            if not re_pat or re_pat.search(str(u)):
                retl += u.fget()
        return retl
    ####################################################

    def load_all_users(self, db):
        ''' :type db: Connection '''
        cur = db.cursor()
        cur.execute(
            'SELECT id AS uid, Name AS name, TaxRateId AS tp_id, Opt AS tp_data_json, version FROM persons WHERE NOT deleted')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            if self.__version < r['version']:
                self.__version = r['version']
            self.__users[r['uid']] = User(r['uid'], r['name'], TP(self.__tps[r['tp_id']], r['tp_data_json']))
        logSys.debug('Now %s users loaded...',len(self.__users))
    ####################################################

    def update(self, db):
        ''' :type db: Connection '''
        c = db.cursor()
        try:
            c.execute('LOCK TABLES persons READ')
            sql = 'SELECT id AS uid, Name AS name, TaxRateId AS tp_id, Opt AS tp_data_json, version, deleted ' \
                  'FROM persons WHERE version > %s ORDER BY version'
            c.execute(sql, (self.__version,))
            if c.rowcount:
                logSys.debug('load info about %s updated users', c.rowcount)
            for u in ({c.description[i][0]: item for (i, item) in en} for en in
                      (enumerate(row) for row in c.fetchall())):
                self.__version = u['version']
                user = self.__users.get(u['uid'],None)
                if user:
                    if int(u['deleted']):
                        del self.__users[u['uid']]
                        logSys.debug('delete user with id: %s',u['uid'])
                        continue
                    # Init new TP with old data
                    user.tp = TP(self.__tps[u['tp_id']], user.tp.json_data)
                    user.name = u['name']
                    logSys.debug('update user with id: %s', u['uid'])
                elif not int(u['deleted']):
                    logSys.debug('create new user with id: %s', u['uid'])
                    self.__users[u['uid']] = User(u['uid'], u['name'], TP(self.__tps[u['tp_id']], u['tp_data_json']))
        finally:
            c.execute('UNLOCK TABLES')
    ####################################################