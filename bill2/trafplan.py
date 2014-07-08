__author__ = 'sn'
from MySQLdb import Connection
import json

class TP:
    def __init__(self, tp_core, param):
        """
        :param tp_core:
         :type tp_core : TPCore
        """
        self.__core = tp_core
        self.__param = dict()
        self.json_data = param

    @property
    def have_limit(self):
        return self.__core.have_limit

    @property
    def json_data(self):
        return json.dumps(self.__param)

    @json_data.setter
    def json_data(self,txt_data):
        try:
            self.__param = json.loads(txt_data)
        except (ValueError, TypeError):
            pass

    def get_user_state_for_nas(self):
        '''
        :return: tuple (user_is_active, upload_speed, download_speed, filter_number)
        '''
        return self.__core.get_state_for_nas(self.__param)

    def calc_traf(self, traf, timestamp):
        return self.__core.calc_traf(traf, timestamp, self.__param)
#####################################################################################


class TPCore:
    def __init__(self, tp_id, name, param):
        self.__id = tp_id
        self.__name = name
        self.__limits = False

    @property
    def have_limit(self):
        return self.__limits

    def test(self, **karg):
        pass

    def get_state_for_nas(self,base):
        '''
        :param base:
        :return: tuple (user_is_active, upload_speed, download_speed, filter_number)
        '''
        return (True,None,None,None)

    def calc_traf(self, traf, timestamp, base):
        '''
        :param traf: (count_up, count_down)
        :param timestamp: datetime
        :param base: dict with traffic plan parameters
        :return: True if user data updated
        '''
        return False
#####################################################################################


class TPFixSpeedCore(TPCore):
    def __init__(self, tp_id, name, param):
        TPCore.__init__(self, tp_id, name, param)
        try:
            self.__param = json.loads(param)
        except (ValueError, TypeError):
            self.__param = dict()
        self.__up = self.__param['speed_up'] if 'speed_up' in self.__param else None
        self.__dw = self.__param['speed_dw'] if 'speed_dw' in self.__param else None

    def get_state_for_nas(self, base):
        return True,self.__up,self.__dw,None


class TrafPlans:
    def __init__(self):
        self.__tps = dict()

    def load_all_tps(self, db):
        cur = db.cursor()
        cur.execute('SELECT id, name, tp_class_name, param FROM traf_planes')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            if r['tp_class_name'] in globals():
                self.__tps[r['id']] = globals()[r['tp_class_name']](r['id'], r['name'], r['param'])
            else:
                self.__tps[r['id']] = TPCore(r['id'], r['name'], r['param'])
        print 'Now %s traf plans loaded...' % len(self.__tps)

    def get_tp(self, id):
        return self.__tps[id] if id in self.__tps else None



