__author__ = 'sn'

from MySQLdb import connect
from threading import Thread,Lock
from config import dbhost, dbuser, dbpass, dbname
from Queue import Queue
from commands import Command


class Users(Thread):
    def __init__(self):
        super(Users,self).__init__()
        self.__users_lock = Lock()
        self.__users = dict()
        self.__db = connect(host=dbhost, user=dbuser, passwd=dbpass, db=dbname,use_unicode=True,charset='cp1251')
        self.__comq = Queue()
        self.loadUsers()

    def appendUser(self,user):
        assert isinstance(user,User)
        with self.__users_lock:
            self.__users[user.uid] = user

    def putCmd(self,cmd):
        assert isinstance(cmd,Command)
        self.__comq.put(cmd)


    def run(self):
        while True:
            item = self.__comq.get()
            assert isinstance(item,Command)
            if item.uid in self.__users :
                print "Command for user id %s received - %s" % (item.uid,item.cmd)



    def loadUsers(self):
        cur = self.__db.cursor()
        cur.execute(("select id as uid,Name as name,"
                     "UnitRem as countDw,UnitRemOut as countUp,"
                     "TaxrateId as tpId,PrePayedUnits as units from persons "
        ))
        for row in cur.fetchall():
            self.__users[row[0]] = User(**{cur.description[n][0]:item for (n,item) in enumerate(row)})



class User:
    def __init__(self,uid,name='',tpId=None,countDw=0,countUp=0,units=0):
        self.__uid = uid
        self.__name = name
        self.__tp = tpId
        self.__countDw = countDw
        self.__countUp = countUp
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
    u.putCmd(Command(82,'qqq',None))