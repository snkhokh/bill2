__author__ = 'sn'
from MySQLdb import connect

class TP:
    def __init__(self,id,name):
        self.__id = id
        self.__name = name

    def have_limit(self):
        return True

    def make_param_for_user(self,**tp_arg):
        return dict(count_in=0, count_out=0)

    def test(self,**karg):
        pass

class Traf_plans:
    def __init__(self):
        self.__tps = dict()

    def load_all_tps(self,db):
        cur = db.cursor()
        cur.execute('SELECT id ,Name AS name FROM tax_rates')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            self.__tps[r['id']] = TP(r['id'],r['name'])
        print 'Now %s traf plans loaded...' % len(self.__tps)

    def get_tp(self,id):
        return self.__tps[id] if id in self.__tps else None


