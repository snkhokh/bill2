__author__ = 'sn'
from MySQLdb import Connection
from datetime import datetime
import json

class TP:
    def __init__(self, tp_core, param):
        """
        :param tp_core:
         :type tp_core : TPCore
        """
        self.__core = tp_core
        self.__param = dict()
        try:
            self.__param = json.loads(param)
        except (ValueError, TypeError):
            pass

    @property
    def have_limit(self):
        return self.__core.have_limit

    @property
    def json_data(self):
        return json.dumps(self.__param)

    @json_data.setter
    def json_data(self, txt_data):
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

    def daily_proc(self, timestamp):
        return self.__core.daily_proc(timestamp,self.__param)
#####################################################################################


class TPCore:
    def __init__(self, tp_id, name, param):
        self.__id = tp_id
        self.__name = name

    @property
    def have_limit(self):
        return False

    def get_state_for_nas(self,base):
        '''
        :param base:
        :return: tuple (user_is_active, upload_speed, download_speed, filter_number)
        '''
        return (True,None,None,None)

    def calc_traf(self, traf, timestamp, base):
        '''
        :type traf: tuple(count_up, count_down)
        :type timestamp: datetime
        :param base: traffic plan parameters
        :type base: dict
        :return: True if user data updated
        '''
        return False

    def daily_proc(self,timestamp,base):
        '''
        :type timestamp: datetime
        :type base: dict
        :return: True if user data updated
        '''
        return False
#####################################################################################


class TrafPlans:
    def __init__(self):
        self.__tps = dict()

    def load_all_tps(self, db):
        cur = db.cursor()
        cur.execute('SELECT id, name, tp_class_name, param FROM traf_planes')
        for row in cur.fetchall():
            r = {cur.description[n][0]: item for (n, item) in enumerate(row)}
            self.__tps[r['id']] = globals().get(r['tp_class_name'], TPCore)(r['id'], r['name'], r['param'])
        print 'Now %s traf plans loaded...' % len(self.__tps)

    def get_tp(self, tp_id):
        return self.__tps.get(tp_id, None)
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
        return True, self.__up, self.__dw, None
#####################################################################################


class TPFloatSpeedWithLimitsCore(TPCore):
    def __init__(self, tp_id, name, param):
        TPCore.__init__(self, tp_id, name, param)
        try:
            self.__param = json.loads(param)
        except (ValueError, TypeError):
            self.__param = dict()
        self.__traf_unit = self.__param.get('traf_unit', 1024 * 1024)
        self.__up = self.__param.get('speed_up', None)
        self.__limit_up = self.__param.get('speed_limit_up', None)
        self.__dw = self.__param.get('speed_dw', None)
        self.__limit_dw = self.__param.get('speed_limit_dw', None)
        self.__day_limit = self.__param.get('day_limit', None)
        self.__week_limit = self.__param.get('week_limit', None)
        self.__month_limit = self.__param.get('month_limit',None)
        self.__filter = self.__param.get('filter',None)

    @property
    def have_limit(self):
        return self.__day_limit or self.__month_limit or self.__week_limit

    def calc_traf(self, traf, timestamp, base):
        c_dw = traf[1]
        if c_dw:
            base['day_counter'] = base.get('day_counter', 0) + c_dw
            base['week_counter'] = base.get('week_counter', 0) + c_dw
            base['month_counter'] = base.get('month_counter', 0) + c_dw
            return True
        return False

    def get_state_for_nas(self, base):
        limit = False
        if self.__day_limit and int(base.get('day_counter', 0)/self.__traf_unit) > self.__day_limit:
            limit = True
        elif self.__week_limit and int(base.get('week_counter', 0)/self.__traf_unit) > self.__week_limit:
            limit = True
        elif self.__month_limit and int(base.get('month_counter', 0)/self.__traf_unit) > self.__month_limit:
            limit = True
        if limit:
            return True, self.__limit_up, self.__limit_dw, self.__filter
        return True, self.__up, self.__dw, self.__filter

    def daily_proc(self, timestamp, base):
        '''
        :type timestamp: datetime
        :param base:
        :return:
        '''
        if not 'last_daily_proc' in base:
            base['last_daily_proc'] = timestamp.toordinal()
            base['day_counter'] = 0
            base['week_counter'] = 0
            base['month_counter'] = 0
            return True
        if not base['last_daily_proc'] == timestamp.toordinal():
            base['last_daily_proc'] = timestamp.toordinal()
            base['day_counter'] = 0
            if timestamp.weekday() == 0:
                base['week_counter'] = 0
            if timestamp.day == 1:
                base['month_counter'] = 0
            return True
        return False
#####################################################################################



