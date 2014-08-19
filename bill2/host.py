# coding=utf-8
import re

__author__ = 'sn'

from threading import Lock
from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection
import datetime

from user import Users, User
from util import net
from util.helpers import getLogger

logSys = getLogger(__name__)



class IpLists(object):
    def __init__(self, default_nas=0):
        self.__default_nas = default_nas
        self.__nas_ip_list = dict(default_nas=dict())
    ####################################################

    def __setitem__(self, key, value):
        self.__nas_ip_list[self.__default_nas][key] = value
    ####################################################

    def __getitem__(self, key):
        return self.__nas_ip_list[self.__default_nas].get(key)
    ####################################################

    def __delitem__(self, key):
        if key in self.__nas_ip_list[self.__default_nas]:
            del self.__nas_ip_list[self.__default_nas]
    ####################################################

    def setip(self,nas,ip,value):
        if not nas in self.__nas_ip_list:
            self.__nas_ip_list[nas] = dict()
        self.__nas_ip_list[nas][ip] = value
    ####################################################

    def getip(self,nas,ip):
        if not nas in self.__nas_ip_list or not ip in self.__nas_ip_list[nas]:
            return None
        else:
            return self.__nas_ip_list[nas][ip]
    ####################################################

    def delip(self,nas,ip):
        if nas in self.__nas_ip_list and ip in self.__nas_ip_list[nas]:
            del self.__nas_ip_list[nas][ip]
    ####################################################
    ####################################################




class IP(object):
    def __init__(self, addr, mask=32, session_ver=0):
        m1 = 32 - mask
        ip = addr >> m1
        ip <<= m1
        assert ip == addr
        self.__addr = addr
        self.__mask = mask
        self.ver = session_ver
        self.__count_is_set = False
        self.__count_in = 0
        self.__count_out = 0

    def __str__(self):
        return 'ip=%s ppp=%s' % (self.ip_s, 'yes' if self.ver else 'no')
    ####################################################


    @property
    def ip_n(self):
        return self.__addr, self.__mask
    ####################################################

    @property
    def ip_s(self):
        return net.ip_ntos(self.__addr, self.__mask)
    ####################################################

    def get_delta(self, counter):
        (d_in, d_out) = (0, 0)
        if self.__count_is_set:
            if counter['dw'] > self.__count_in:
                d_in = counter['dw'] - self.__count_in
            if counter['up'] > self.__count_out:
                d_out = counter['up'] - self.__count_out
        else:
            self.__count_is_set = True
        self.__count_in = counter['dw']
        self.__count_out = counter['up']
        return d_in, d_out
    ####################################################

    def counter_reset(self):
        self.__count_is_set = False
    ####################################################
    ####################################################


class Host(dict):
    def __init__(self, db_id, name, user, flags, version, pool=0):
        ''' :type user: User '''
        self.db_id = db_id
        self.pool_id = pool
        self.ver = version
        self.name = name
        self.flags = flags
        self.__user = user
        self.__count_in = 0
        self.__count_out = 0
        self.__ips = dict()
    ####################################################

    def __getitem__(self, key):
        '''
        :rtype: IP
        '''
        return self.get(key)
    ####################################################

    @property
    def user(self):
        return self.__user
    ####################################################

    @user.setter
    def user(self, user_link):
        assert isinstance(user_link, User)
        self.__user = user_link
    ####################################################
    ####################################################


class Hosts(dict):
    def __init__(self, users):
        '''
        :type users: Users
        '''
        self.__ip_lists = IpLists()
        self.__users = users
        self.__version = 0
        self.__sessions_ver = 0
        self.__dbid_to_hosts = dict()
    ####################################################

    @property
    def users(self):
        return self.__users
    ####################################################

    def __getitem__(self, key):
        '''
        :rtype: Host
        '''
        return super(Hosts, self).__getitem__(key)
    ####################################################

    def __setitem__(self, key, value):
        assert isinstance(value, Host)
        self[key] = value
    ####################################################

    def fget(self, mask):
        re_pat = re.compile(mask) if mask else None
        return tuple(str(h) for hid, h in self.items() if not re_pat or re_pat.search(str(h)))

    ####################################################

    def get_hosts_needs_stat(self):
        return (ip for h in self for ip in h if not ip.ver)

    #####################################################

    def load_all_hosts(self, db):
        ''' :type db: Connection '''
        c = db.cursor()
        try:
            c.execute('LOCK TABLES hostip READ, ip_sessions READ')
            #
            sql = 'SELECT id, int_ip, mask, Name, dynamic, flags, PersonId, version FROM hostip WHERE NOT deleted'
            c.execute(sql)
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                user = self.users[h['PersonId']]
                if user:
                    if int(h['dynamic']):
                        self[h['id']] = Host(h['id'], h['Name'], user, h['flags'], h['version'], h['int_ip'])
                    else:
                        host = Host(h['id'], h['Name'], user, h['flags'], h['version'])
                        ip = IP(h['int_ip'], h['mask'])
                        host[net.ip_ntos(h['int_ip'], h['mask'])] = ip
                        self[h['id']] = host
                        self.__ip_lists[ip.ip_s] = host
                if self.__version < h['version']:
                    self.__version = h['version']
            # загрузим инфу о сессиях
            sql = 'SELECT ip_pool_id, framed_ip, in_octets, out_octets, ' \
                  'acc_uid, acc_hid, unix_timestamp(start_time) AS version FROM ip_sessions WHERE stop_time IS NULL' \
                  ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
            c.execute(sql)
            for row in c.fetchall():
                h = {c.description[n][0]: item for (n, item) in enumerate(row)}
                host = self[h['acc_hid']]
                if host and host.user.db_id == h['acc_uid']:
                    ip_id = net.ip_ntos(h['framed_ip'])
                    if not ip_id in host:
                        ip = IP(h['framed_ip'], 32, h['version'])
                        host[ip_id] = ip
                        self.__ip_lists[ip.ip.ip_s] = host
                    elif h['version'] >= host[ip_id].ver:
                        host[ip_id].counter_reset()
                    host[ip_id].get_delta(dict(dw=h['out_octets'], up=h['in_octets']))
                    #
        finally:
            c.execute('UNLOCK TABLES')
        logSys.debug('info about %s hosts loaded', len(self.__hosts))
    #####################################################

    def update_hosts(self, db):
        '''
        :type db: Connection
        :return:
        '''
        c = db.cursor()
        try:
            c.execute('LOCK TABLES hostip READ')
            sql = 'SELECT id, int_ip, mask, Name, dynamic, flags, PersonId, version, deleted FROM hostip WHERE version > %s' \
                  ' ORDER BY version'
            c.execute(sql, self.__version)
            if c.rowcount:
                logSys.debug('loading info about %s updated hosts', c.rowcount)
            for h in ({c.description[i][0]: item for (i, item) in en} for en in
                      (enumerate(row) for row in c.fetchall())):
                self.__version = h['version']
                host = self[h['id']]
                user = self.users[h['PersonId']]
                dynamic = int(h['dynamic'])
                if host:
                    if int(h['deleted']):
                        logSys.debug('delete host id: %s', h['id'])
                        for ip in self[h['id']]:
                            del self.__ip_lists[ip]
                        del self[h['id']]
                        continue
                    host.name = h['Name']
                    host.flags = h['flags']
                    host.ver = h['version']
                    if user and host.user.db_id != user.db_id:
                        host.user = user
                    if host.pool_id:
                        #  хост был на динамике
                        if not dynamic:
                            # изменение на статику
                            for ip in host:
                                del self.__ip_lists[ip]
                            host.clear()
                            host[net.ip_ntos(h['int_ip'], h['mask'])] = IP(h['int_ip'], h['mask'])
                            host.pool_id = 0
                        elif host.pool_id != h['int_ip']:
                            # изменился номер пула
                            host.pool_id = h['int_ip']
                    else:
                        # хост был на статике
                        if dynamic:
                            # изменение на динамику
                            for ip in host:
                                del self.__ip_lists[ip]
                            host.clear()
                            host.pool_id = h['int_ip']
                        elif host[host.keys()[0]].ip_n != (h['int_ip'], h['mask']):
                            # изменение ip статической записи
                            ip_id = host.keys()[0]
                            del host[ip_id]
                            del self.__ip_lists[ip_id]
                            ip = IP(h['int_ip'], h['mask'])
                            host[ip.ip_s] = ip
                            self.__ip_lists[ip.ip.ip_s] = host
                elif int(h['deleted']):
                    continue
                elif user:
                    logSys.debug('new host with id: %s', h['id'])
                    if dynamic:
                        self[h['id']] = Host(h['id'], h['Name'], user, h['flags'], h['version'], h['int_ip'])
                    else:
                        host = Host(h['id'], h['Name'], user, h['flags'], h['version'])
                        ip = IP(h['int_ip'], h['mask'])
                        host[net.ip_ntos(h['int_ip'], h['mask'])] = ip
                        self[h['id']] = host
                        self.__ip_lists[ip.ip_s] = host
        finally:
            c.execute('UNLOCK TABLES')
    #####################################################

    def update_sessions(self, db):
        '''
        :type db: Connection
        '''
        c = db.cursor()
        sessions = {ip.ip_s: ip.ver for h in self for ip in h if ip.ver}
        sql = 'SELECT ip_pool_id,framed_ip,in_octets,out_octets,' \
              'acc_uid, acc_hid, unix_timestamp(start_time) AS version FROM ip_sessions WHERE stop_time IS NULL' \
              ' AND l_update > date_sub(now(),INTERVAL 6 MINUTE)'
        c.execute(sql)
        for row in c.fetchall():
            h = {c.description[n][0]: item for (n, item) in enumerate(row)}
            host = self[h['acc_hid']]
            user = self.users[h['acc_uid']]
            if user and host and user.db_id == host.user.db_id:
                ip_id = h['framed_ip']
                old_ver = sessions.pop(ip_id) if ip_id in sessions else None
                if host.pool_id:
                    # динамический адрес
                    if not ip_id in host:
                        if ip_id in self.__ip_lists:
                            del self.__ip_lists[ip_id][ip_id]
                        ip = IP(h['framed_ip'], 32, h['version'])
                        host[ip_id] = ip
                        self.__ip_lists[ip_id] = host
                    elif old_ver and old_ver != h['version']:
                        # новая сессия
                        host[ip_id].counter_reset()
                else:
                    # статический адрес
                    host = self.__hosts[host_id]
            if not old_ver or not old_ver == h['version']:
                host.is_ppp = True
                host.user = self.__users[h['acc_uid']]
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

    def update_stat_for_hosts(self, hosts):
        for h, cnt in hosts:
            if h in self.__hosts and not self.__hosts[h].is_ppp:
                self.__hosts[h].counter = cnt
    #####################################################

    def prepare_state(self):
        '''
        :return: dict { host_id: (host_is_active, upload_speed, download_speed, filter_number)
        :rtype: dict
        '''
        return {h: (self[h].user.tp.get_user_state_for_nas()) for h in self}
    #####################################################
