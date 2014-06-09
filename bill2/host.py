__author__ = 'sn'

import threading
from MySQLdb.cursors import Cursor
from user import Users


class Hosts():
    def __init__(self,users):
        """

        :type users: Users
        """
        self.__users = users
        self.__lock = threading.Lock()
        self.__hosts = dict()

    def getHost(self,hostId):
        with self.__lock:
            if hostId in self.__hosts: return dict(zip(('nas','host'),self.__hosts[hostId]))
            else: return None

    def createHost(self,hostId,nas):
        with self.__lock:
            if hostId in self.__hosts:
                (tnas,thost) = self.__hosts[hostId]
                if id(tnas) <> id(nas) : raise RuntimeError
                else: return thost
            thost  = Host(hostId,nas.db)
            self.__hosts[hostId] = (nas,thost)
            return thost

    def removeHost(self,hostId):
        with self.__lock:
            del self.__hosts[hostId]


def getStaticHostInfo(ip, db):
    cur = db.cursor()
    assert isinstance(cur, Cursor)
    cur.execute('SELECT PersonId,mask,flags from hostip where int_ip = %s',(ip,))
    row = cur.fetchone()
    if not row: return None
    r = dict()
    r['uid'] = row[0]
    r['lprefix']=row[1]
    r['flags'] = row[2]
    return r




def getDynHostInfo(ip, pool, db):
    pass


class Host(object):
    def __init__(self, id, db):
        """

        :param cursor:
        :rtype : Host
        :type tp: trafplan.TP
        """
        (pool,ip) = id.split('_')
        self.ip = int(ip)
        self.pool = int(pool)
        if not self.pool:
            info = getStaticHostInfo(ip,db)
        else:
            info = getDynHostInfo(ip,pool,db)
        if not info: raise
        self.lprefix = info['lprefix']
        self.uid = info['uid']
        #a = getCurPolicyForUser(self.uid)
        #self.curSpeedDw,
        #self.curSpeedUp,
        #self.curFilterId) = info
        self.count_in = 0
        self.count_out = 0

    

    def test(self):
        self.__tp.test(self)

