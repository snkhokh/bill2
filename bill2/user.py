__author__ = 'sn'

from MySQLdb import connect
from threading import Thread, Lock
from config import dbhost, dbuser, dbpass, dbname
from Queue import Queue
from commands import Command


class Users():
    def __init__(self):
        self.__users_lock = Lock()
        self.__users = dict()
        self.__db = connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname, use_unicode=True, charset='cp1251')
        self.__comq = Queue()
        self.loadUsers()

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


    def loadUsers(self):
        cur = self.__db.cursor()
        cur.execute(
            'SELECT id AS uid,Name AS name,UnitRem AS cnt_dw, UnitRemOut AS cnt_up,TaxRateId AS tp_id,'
            'PrePayedUnits AS units FROM persons ')
        for row in cur.fetchall():
            self.__users[row[0]] = User(**{cur.description[n][0]: item for (n, item) in enumerate(row)})
        print 'Now %s users loaded...' % len(self.__users)


class User:
    def __init__(self, uid, name='', tp_id=None, cnt_dw=0, cnt_up=0, units=0):
        self.__uid = uid
        self.__name = name
        self.__tp = tp_id
        self.__countDw = cnt_dw
        self.__countUp = cnt_up
        self.__units = units

    @property
    def uid(self):
        return self.__uid

    def isOn(self):
        if self.__units: return True
        return False


if __name__ == '__main__':
    u = Users()
    u.start()
    print 'Done'
    u.putCmd(Command(82, 'qqq', None))