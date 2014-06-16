# coding=utf-8
from macpath import split

__author__ = 'sn'

from threading import Thread, Timer
from Queue import Queue, Empty


from host import Hosts
from user import Users
from commands import Command
from config import periodic_proc_timeout


class Nas(Thread):

    def ip_ntos(n,prefix=32):
        tail = '/'+str(prefix) if prefix < 32 else ''
        return str(n>>24 & 255)+'.'+str(n>>16 & 255)+'.'+str(n>>8 &255)+'.'+str(n & 255) + tail

    def ip_ston(addr):
        s = addr.split('/')
        lpref = int(s.pop()) if len(s) > 1 else 32
        x = int(0)
        for i in s.split('.'):
            x <<= 8
            x += int(i)
        return (x,lpref)

    def __init__(self, hosts=None):
        """
        :type hosts: Hosts
        :type users: Users
        """
        super(Nas, self).__init__()
        self.__all_hosts = hosts
        self.__comq = Queue()
        self.__exit_flag = False


    def get_host(self, hostId):
        return self.__all_hosts.get_host(hostId)

# Command handlers

    def host_on(self, cmd):
        assert isinstance(cmd, Command)
        h = self.get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        if not hwState['on']:
            if self._hw_host_on(h): h.on = True

    def host_off(self, cmd):
        h = self.get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        if hwState['on']:
            if self._hw_host_off(h): h.on = False

    def host_rm(self, cmd):
        hid = cmd.hostid
        host = self.get_host(hid)
        self._hw_host_off(hid)
        # do some with host
        self.__all_hosts.remove_host(hid)

    def set_host_speed(self, cmd):
        assert isinstance(cmd,Command)
        h = self.get_host(cmd.hostid)
        hwState = self._hw_get_host_state(h)
        a = cmd.params
        if 'speedUp' in cmd.params and hwState['spUp'] <> cmd.params['speedUp']:
            if self._hw_set_host_speed(h, 'up', cmd.params['speedUp']):
                h.curSpeedUp = cmd.params['speedUp']
        if 'speedDw' in cmd.params and hwState['spDw'] <> cmd.params['speedDw']:
            if self._hw_set_host_speed(h, 'down', cmd.params['speedDw']):
                h.curSpeedUp = cmd.params['speedDw']

    def setHostFilter(self, cmd):
        h = self.get_host(cmd.hostid)
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
            Timer(periodic_proc_timeout, self.__timer_handler).start()
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

