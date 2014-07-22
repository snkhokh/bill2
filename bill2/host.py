# coding=utf-8
__author__ = 'sn'

from threading import Lock
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection
import datetime

from user import Users, User
from util import net
from util.helpers import getLogger

logSys = getLogger(__name__)


class Host(object):
    def __init__(self, host_ip, lprefix):
        # check ip and mask before create host
        m1 = 32 - lprefix
        ip = host_ip >> m1
        ip <<= m1
        assert ip == host_ip
        #
        self.__ip = host_ip
        self.__lprefix = lprefix
        self.__pool_id = 0
        self.__user = None
        self.__version = 0
        self.__count_in = 0
        self.__count_out = 0
        self.__stat_count_in = 0
        self.__stat_count_out = 0
        self.__stat_is_set = False
        self.__db_id = 0
        #
        self.is_ppp = False
        self.session_ver = 0
    ####################################################

    @property
    def db_id(self):
        return self.__db_id
    ####################################################

    @db_id.setter
    def db_id(self, db_id):
        self.__db_id = db_id
    ####################################################

    @property
    def pool_id(self):
        return self.__pool_id
    ####################################################

    @pool_id.setter
    def pool_id(self, n):
        self.__pool_id = n
    ####################################################

    @property
    def ip_n(self):
        return self.__ip, self.__lprefix
    ####################################################

    @property
    def ip_s(self):
        return net.ip_ntos(self.__ip, self.__lprefix)
    ####################################################

    @property
    def ver(self):
        return self.__version
    ####################################################

    @ver.setter
    def ver(self, n):
        self.__version = n
    ####################################################

    @property
    def user(self):
        return self.__user
    ####################################################

    @user.setter
    def user(self, user_link):
        if not user_link is None:
            assert isinstance(user_link, User)
        self.__user = user_link
    ####################################################

    @property
    def counter(self):
        return (self.__count_in, self.__count_out) if self.__stat_is_set else (0, 0)
    ####################################################

    @counter.setter
    def counter(self, c):
        if self.__stat_is_set:
            d_in = c['dw'] - self.__stat_count_in
            if d_in > 0:
                self.__count_in += d_in
            d_out = c['up'] - self.__stat_count_out
            if d_out > 0:
                self.__count_out += d_out
        else:
            self.__stat_is_set = True
        self.__stat_count_in = c['dw']
        self.__stat_count_out = c['up']
    ####################################################

    def counter_reset(self):
        self.__count_in = 0
        self.__count_out = 0
####################################################
####################################################


class Hosts():
    def __init__(self, users):
        '''
        :type users: Users
        '''
        self.__users = users
        self.__version = 0
        self.__sessions_ver = 0
        self.__lock = Lock()
        self.__hosts = dict()
        self.__dbid_to_hosts = dict()
    ####################################################

    def get_host(self, host_id):
        '''
        :param host_id:
        :return:
        :rtype: Host
        '''
        return self.__hosts[host_id]
    ####################################################

    def get_hosts_needs_stat(self):
        with self.__lock:
            return (h for h in self.__hosts.keys() if not self.__hosts[h].is_ppp)
    #####################################################

    def load_all_hosts(self,db):
        '''
        :type db: Connection
        :return:
        '''
        c = db.cursor()
        # загрузим инфу о сессиях
        sql = 'SELECT ip_pool_id,framed_ip AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) AS version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = h['host_ip']
                if not host_id in self.__hosts:
                    host = Host(*net.ip_ston(h['host_ip']))
                    self.__hosts[host_id] = host
                    host.is_ppp = True
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                elif h['version'] >= self.__hosts[host_id].session_ver:
                    # новая сессия от уже обработанного IP
                    host = self.__hosts[host_id]
                    host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
                    host.counter_reset()
                host.user = self.__users.get_user(h['acc_uid'])
                host.pool_id = h['ip_pool_id']
                host.session_ver = h['version']
                #
        try:
            c.execute('LOCK TABLES hostip READ')
            #
            sql = 'SELECT id, int_ip, mask, PersonId, version FROM hostip WHERE NOT deleted AND dynamic = 0'
            c.execute(sql)
            with self.__lock:
                for row in c.fetchall():
                    h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                    host_id = net.ip_ntos(h['int_ip'], h['mask'])
                    self.__dbid_to_hosts[h['id']] = host_id
                    if not host_id in self.__hosts:
                        host = Host(h['int_ip'], h['mask'])
                        host.user = self.__users.get_user(h['PersonId'])
                        self.__hosts[host_id] = host
                    else:
                        host = self.__hosts[host_id]
                    host.db_id = h['id']
                    host.ver = h['version']
                    if self.__version < h['version']:
                        self.__version = h['version']
        finally:
            c.execute('UNLOCK TABLES')
    #####################################################

    def update_hosts(self, db):
        '''
        :type db: Connection
        :return:
        '''
        c = db.cursor()
        try:
            c.execute('LOCK TABLES hostip READ')
            sql = 'SELECT id, int_ip, mask, PersonId, version, deleted FROM hostip WHERE dynamic = 0 AND version > %s' \
                  ' ORDER BY version'
            c.execute(sql, self.__version)
            with self.__lock:
                for h in ({c.description[i][0]: item for (i, item) in en} for en in
                          (enumerate(row) for row in c.fetchall())):
                    self.__version = h['version']
                    new_host_id = net.ip_ntos(h['int_ip'], h['mask'])
                    old_host_id = self.__dbid_to_hosts.get('id', None)
                    if old_host_id:
                        if h['deleted']:
                            del self.__hosts[old_host_id]
                            del self.__dbid_to_hosts[h['id']]
                            continue
                        user = self.__users.get_user(h['PersonId'])
                        if new_host_id != old_host_id:
                            self.__dbid_to_hosts[h['id']] = new_host_id
                            del self.__hosts[old_host_id]
                            host = Host(h['int_ip'], h['mask'])
                            host.db_id = h['id']
                            self.__hosts[new_host_id] = host
                        else:
                            host = self.__hosts[new_host_id]
                        host.user = user
                        host.ver = h['version']
                    else:
                        logSys.error('host with db id: %s (new host id: %s) not found!', h['id'], new_host_id)
        finally:
            c.execute('UNLOCK TABLES')


    def update_sessions(self, db):
        '''
        :type db: Connection
        :return:
        '''
        c = db.cursor()
        assert isinstance(c, Cursor)
        sql = 'SELECT ip_pool_id,framed_ip AS host_ip,in_octets,out_octets,' \
              'acc_uid,unix_timestamp(start_time) AS version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        with self.__lock:
            sessions = {item: self.__hosts[item].session_ver for item in self.__hosts.keys()
                        if self.__hosts[item].session_ver}
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host_id = h['host_ip']
                old_ver = sessions.pop(host_id) if host_id in sessions else None
                if not host_id in self.__hosts:
                    host = Host(*net.ip_ston(h['host_ip']))
                    self.__hosts[host_id] = host
                else:
                    host = self.__hosts[host_id]
                if not old_ver or not old_ver == h['version']:
                    host.is_ppp = True
                    host.user = self.__users.get_user(h['acc_uid'])
                    host.session_ver = h['version']
                    host.pool_id = h['ip_pool_id']
                elif old_ver and not old_ver == h['version']:
                    # todo сбросить статистику в базу
                    host.counter = dict(dw=0, up=0)
                    host.counter_reset()
                host.counter = dict(dw=h['out_octets'], up=h['in_octets'])
            for host_id in sessions.keys():
                # для этих хостов сессия звыершена
                self.__hosts[host_id].session_ver = 0
    ####################################################

    def do_billing(self, db):
        '''
        :type db: Connection
        :return:
        '''
        now = datetime.datetime.now()
        stat_cnt = dict()
        upd_users = set()
        with self.__lock:
            for host_id in self.__hosts:
                host = self.__hosts[host_id]
                c = host.counter
                if c[0] + c[1]:
                    k = (host.user.db_id, host.db_id)
                    if not k in stat_cnt:
                        stat_cnt[k] = c
                    else:
                        stat_cnt[k] = tuple(c[i] + cnt for (i, cnt) in enumerate(stat_cnt[k]))
                    host.counter_reset()
                    if host.user.tp.have_limit:
                        if host.user.tp.daily_proc(now):
                            upd_users.add(host.user.db_id)
                        if host.user.tp.calc_traf(c, now):
                            upd_users.add(host.user.db_id)
        cur = db.cursor()
        if stat_cnt:
            for k in stat_cnt.keys():
                cur.execute('INSERT INTO stat (user_id, host_id, ts, dw, up) VALUES (%s, %s, %s, %s, %s)',
                            k + (now,) + stat_cnt[k])
        for user_id in upd_users:
            self.__users.get_user(user_id).db_upd_tp_data(cur)
    #####################################################

    def update_stat_for_hosts(self, hosts):
        with self.__lock:
            for h, cnt in hosts:
                if h in self.__hosts and not self.__hosts[h].is_ppp:
                    self.__hosts[h].counter = cnt
    #####################################################

    def prepare_state(self):
        '''
        :return: dict { host_id: (host_is_active, upload_speed, download_speed, filter_number)
        :rtype: dict
        '''
        with self.__lock:
            return {h: (self.get_host(h).user.tp.get_user_state_for_nas()) for h in self.__hosts.keys()}
    #####################################################
