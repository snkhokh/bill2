# coding=utf-8
__author__ = 'sn'

from threading import Thread, Timer
from Queue import Queue, Empty


from host import Hosts
from commands import Command
from config import nas_stat_update_period, nas_conf_update_period
from util.helpers import getLogger

logSys = getLogger(__name__)


class Nas(Thread):
    def __init__(self, hosts):
        """
        :type hosts: Hosts
        """
        super(Nas, self).__init__()
        self.__hosts = hosts
        self.__comq = Queue()
        self.__exit_flag = False
        self._nas_connected = False

    @property
    def _connect(self):
        if not self._nas_connected:
            self._nas_connected = True
        return True

    def do_stats(self, cmd=None):
        if self._connect:
            try:
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
                self.__hosts.update_stat_for_hosts(self._get_hw_stats())
            except NasComError:
                self._nas_connected = False
#
        Timer(nas_stat_update_period, self.queue_do_stats).start()
############################################################

    def queue_do_stats(self):
        self.put_cmd(Command('do_stats'))
############################################################

    def update_conf(self, cmd=None):
        if self._connect:
            try:
                hosts_to_set = dict()
                hosts_to_unset = self.get_config()
                for (h_ip, state) in self.__hosts.prepare_state().items():
                    if h_ip in hosts_to_unset:
                        hw_state = hosts_to_unset.pop(h_ip)
                        if not hw_state == state:
                            hosts_to_set[h_ip] = state
                    else:
                        hosts_to_set[h_ip] = state
                for (ip, state) in hosts_to_set.items():
                    self._set_host_state(ip, state)
                for ip in hosts_to_unset.keys():
                    self._unreg_host(ip)
            except NasComError:
                self._nas_connected = False
#
        Timer(nas_conf_update_period, self.queue_update_conf).start()
############################################################

    def queue_update_conf(self):
        self.put_cmd(Command('update_conf'))
############################################################

# Command handlers

    def do_exit(self, cmd):
        self.__exit_flag = True

    cmd_router = {'stop': do_exit,
                  'update_conf': update_conf,
                  'do_stats': do_stats}

    def run(self):
        logSys.info('Nas started...')
        self.do_stats()
        self.update_conf()
        while not self.__exit_flag:
            try:
                cmd = self.__comq.get(timeout=1)
                assert isinstance(cmd, Command)
                if cmd.cmd in Nas.cmd_router:
                    logSys.debug('Cmd: ''%s'' received', cmd.cmd)
                    Nas.cmd_router[cmd.cmd](self, cmd.uid)
            except Empty:
                pass
        logSys.info('Nas done!!!')

    def put_cmd(self, cmd):
        self.__comq.put(cmd)

#Hardware specific functions

    def _hw_set_stats_for_hosts(self, hosts_to_set):
        pass

    def _hw_unset_stats_for_hosts(self, hosts_to_unset):
        pass

    def _hw_get_hosts_stats_set(self):
    #abstract method
        return set()

    def get_config(self):
        pass

    def _set_host_state(self, ip, state):
        pass

    def _unreg_host(self, ip):
        pass

    def _hw_update_stats(self):
        pass

    def _get_hw_stats(self):
        return list()


class NasComError(Exception):
    pass