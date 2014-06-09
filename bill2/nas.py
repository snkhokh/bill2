# coding=utf-8
from macpath import split

__author__ = 'sn'

from threading import Thread, Timer
from Queue import Queue, Empty

from MySQLdb.cursors import Cursor
from MySQLdb.connections import Connection

from host import Hosts
from user import Users
from commands import Command
from config import dbhost, dbuser, dbpass, dbname


class Nas(Thread):

    def ip_stoa(n):
        return str(n>>24 & 255)+'.'+str(n>>16 & 255)+'.'+str(n>>8 &255)+'.'+str(n & 255)

    def ip_aton(s):
        x = int(0)
        for i in s.split('.'):
            x <<= 8
            x += int(i)
        return x

    def __init__(self, hosts=None, users=None):
        """
        :type hosts: Hosts
        :type users: Users
        """
        super(Nas, self).__init__()
        self.__all_hosts = hosts
        self.__users = users
        self.__hosts = dict()
        self.__db = Connection(host=dbhost, user=dbuser, passwd=dbpass, db=dbname)
        self.__comq = Queue()
        self.loadAllStaticHosts()
        self.__exit_flag = False


    @property
    def db(self):
        return self.__db

    def loadAllStaticHosts(self):
        c = self.__db.cursor()
        assert isinstance(c, Cursor)
        # ToDo в базе д.б. признак привязки статика к конкретному НАС
        c.execute('SELECT int_ip FROM hostip WHERE dynamic = 0')
        for row in c.fetchall():
            self.__get_host('0_' + str(row[0]))

    def __get_host(self, hostId):
        if not hostId in self.__hosts:
            host = self.__all_hosts.createHost(hostId, self)
            self.__hosts[hostId] = host
        return self.__hosts[hostId]

# Command handlers

    def host_on(self, cmd):
        assert isinstance(cmd, Command)
        h = self.__get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        if not hwState['on']:
            if self._hw_host_on(h): h.on = True

    def host_off(self, cmd):
        h = self.__get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        if hwState['on']:
            if self._hw_host_off(h): h.on = False

    def host_rm(self, cmd):
        hid = cmd.hostid
        self._hw_host_off(hid)
        host = self.__hosts[hid]
        # do some with host
        self.__all_hosts.removeHost(hid)
        del self.__hosts[hid]

    def set_host_speed(self, cmd):
        assert isinstance(cmd,Command)
        h = self.__get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        a = cmd.params
        if 'speedUp' in cmd.params and hwState['spUp'] <> cmd.params['speedUp']:
            if self._hw_set_host_speed(h, 'up', cmd.params['speedUp']):
                h.curSpeedUp = cmd.params['speedUp']
        if 'speedDw' in cmd.params and hwState['spDw'] <> cmd.params['speedDw']:
            if self._hw_set_host_speed(h, 'down', cmd.params['speedDw']):
                h.curSpeedUp = cmd.params['speedDw']

    def setHostFilter(self, cmd):
        h = self.__get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        if 'filterId' in cmd.params:
            filterId = cmd.params['filterId']
            if hwState['filter'] <> filterId and self._hw_set_host_filter(h, filterId):
                h.curFilterId = filterId


    def do_exit(self, cmd):
        isinstance(cmd,Command)
        print 'Stop cmd received!!!'
        self.__exit_flag = True

    def __do_timer(self, cmd):
        self.periodic_proc()
        self.__timer_handler(True)

    def __timer_handler(self, i=None):
        if i:
            Timer(6, self.__timer_handler).start()
        else:
            self.putCmd(Command('timer'))



    cmd_router = {'stop': do_exit,
                  'hoston': host_on,
                  'hostoff': host_off,
                  'timer': __do_timer}

    def run(self):
        print 'Nas started...'
        self.__timer_handler(True)
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in Nas.cmd_router: Nas.cmd_router[cmd.cmd](self, cmd.uid)
            except Empty:
                pass
        print 'Nas done!!!'

    def putCmd(self, cmd):
        self.__comq.put(cmd)

    def periodic_proc(self):
        print "Periodic procedure!!!"

#Hardware specific functions

    def _hw_host_on(self, host):
        return True

    def _hw_host_off(self, host):
        return True

    def _hw_get_host_state(self, host):
        return {'cin': 0, 'cout': 0, 'on': False, 'filter': None, 'spDw': None, 'spUp': None}

    def _hw_set_host_speed(self, h, param, speedUp):
        pass

    def _hw_set_host_filter(self, h, filterId):
        pass

