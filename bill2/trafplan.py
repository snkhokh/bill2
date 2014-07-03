__author__ = 'sn'
from MySQLdb import connect
import json

class TP:
    def __init__(self,id,name,param):
        self.__id = id
        self.__name = name

    def have_limit(self):
        return True

    def load_data(self,data_json):
        try:
            tp_data = json.loads(data_json)
        except (ValueError, TypeError):
            return dict(count_in=0, count_out=0)
        return tp_data

    def save_data(self, data):
        return json.dumps(data)

    def test(self,**karg):
        pass

    def calc_traf(self, traf, timestamp, base):
        (bytes_dw, bytes_up) = traf
        base['count_in'] += bytes_dw
        base['count_out'] += bytes_up




class Traf_plans:
    def __init__(self):
        self.__tps = dict()

    def load_all_tps(self,db):
        cur = db.cursor()
        cur.execute('SELECT id ,name, param FROM traf_planes')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            self.__tps[r['id']] = TP(r['id'],r['name'],r['param'])
        print 'Now %s traf plans loaded...' % len(self.__tps)

    def get_tp(self,id):
        return self.__tps[id] if id in self.__tps else None


