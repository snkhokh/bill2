# coding=utf-8
__author__ = 'sn'

from threading import Thread, Timer
from Queue import Queue, Empty


from host import Hosts
from commands import Command
from config import nas_stat_update_period, nas_conf_update_period


class Nas(Thread):
    def __init__(self, hosts=None):
        """
        :type hosts: Hosts
        """
        super(Nas, self).__init__()
        self.__hosts = hosts
        self.__comq = Queue()
        self.__exit_flag = False

    def do_stats(self,cmd=None):
        print 'Update nas stats...'
        # hosts with nas synchronization
        hosts_to_set = set()
        hosts_to_unset = self._hw_get_hosts_stats_set()
        for h_ip in self.__hosts.get_hosts_needs_stat():
            if h_ip in hosts_to_unset:
                hosts_to_unset.remove(h_ip)
            else:
                hosts_to_set.add(h_ip)
        if hosts_to_set:
            self._hw_set_stats_for_hosts(hosts_to_set)
        if hosts_to_unset:
            self._hw_unset_stats_for_hosts(hosts_to_unset)
        # collect stats and send to hosts
        self._hw_update_stats()
        #
        print 'Send nas stats to hosts...'
        self.__hosts.update_stat_for_hosts(self._get_hw_stats())
        print 'Stats proc done...'
#
        Timer(nas_stat_update_period, self.queue_do_stats).start()
############################################################

    def queue_do_stats(self):
        self.putCmd(Command('do_stats'))
############################################################

    def update_conf(self, cmd=None):
        print 'Update nas configuration...'
        self.test_reg_hosts()
#
        Timer(nas_conf_update_period, self.queue_update_conf).start()
############################################################

    def queue_update_conf(self):
        self.putCmd(Command('update_conf'))
############################################################

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
############################################################

# Command handlers

    def do_exit(self, cmd):
        isinstance(cmd, Command)
        print 'Stop cmd received!!!'
        self.__exit_flag = True

    cmd_router = {'stop': do_exit,
                  'update_conf': update_conf,
                  'do_stats': do_stats}

    def run(self):
        print 'Nas started...'
        self.do_stats()
        self.update_conf()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in Nas.cmd_router:
                    Nas.cmd_router[cmd.cmd](self, cmd.uid)
            except Empty:
                pass
        print 'Nas done!!!'

    def putCmd(self, cmd):
        self.__comq.put(cmd)

#Hardware specific functions

    def _hw_set_stats_for_hosts(self, hosts_to_set):
        pass

    def _hw_unset_stats_for_hosts(self, hosts_to_unset):
        pass

    def _hw_get_hosts_stats_set(self):
    #abstract method
        return set()

    def _get_reg_hosts_set(self):
        pass

    def _reg_hosts(self, hosts_to_reg):
        pass

    def _unreg_hosts(self, hosts_to_unreg):
        pass

    def _hw_update_stats(self):
        pass

    def _get_hw_stats(self):
        return list()