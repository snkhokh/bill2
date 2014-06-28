# coding=utf-8
from macpath import split

__author__ = 'sn'

from threading import Thread, Timer
from Queue import Queue, Empty


from host import Hosts,Host
from user import Users
from commands import Command
from config import periodic_proc_timeout


class Nas(Thread):

    def __init__(self, hosts=None):
        """
        :type hosts: Hosts
        :type users: Users
        """
        super(Nas, self).__init__()
        self.__hosts = hosts
        self.__comq = Queue()
        self.__exit_flag = False


    def test_statis_hosts(self):
        hosts_to_set = set()
        hosts_to_unset = self._get_statis_hosts_set()
        for h_ip in self.__hosts.get_hosts_needs_stat():
            if h_ip in hosts_to_unset:
                hosts_to_unset.remove(h_ip)
            else:
                hosts_to_set.add(h_ip)
        if hosts_to_set:
            self._set_statis_hosts(hosts_to_set)
        if hosts_to_unset:
            self._unset_statis_hosts(hosts_to_unset)

    def test_reg_hosts(self):
        hosts_to_reg = set()
        hosts_to_unreg = self._get_reg_hosts_set()
        for h_ip in self.__hosts.get_reg_hosts():
            if h_ip in hosts_to_unreg:
                hosts_to_unreg.remove(h_ip)
            else:
                hosts_to_reg.add(h_ip)
        if hosts_to_reg:
            self._reg_hosts(hosts_to_reg)
        if hosts_to_unreg:
            self._unreg_hosts(hosts_to_unreg)






# Command handlers



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
        print "Nas periodic procedure start..."
        self.test_statis_hosts()
        self.test_reg_hosts()
        print "Nas periodic procedure done!!!"


#Hardware specific functions

    def _hw_host_on(self, host):
        return True

    def _hw_host_off(self, host):
        return True

    def _hw_set_host_speed(self, h, param, speedUp):
        pass

    def _hw_set_host_filter(self, h, filterId):
        pass

    def _set_statis_hosts(self, hosts_to_set):
        pass

    def _unset_statis_hosts(self, hosts_to_unset):
        pass

    def _get_statis_hosts_set(self):
    #abstract method
        return set()

    def _get_reg_hosts_set(self):
        pass

    def _reg_hosts(self, hosts_to_reg):
        pass

    def _unreg_hosts(self, hosts_to_unreg):
        pass

