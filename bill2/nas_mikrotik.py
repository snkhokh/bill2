__author__ = 'sn355_000'
from bill2.nas import Nas
from util.RosAPI import Core


class MikroNas(Nas):
    def __init__(self, hosts=None, users=None, address='', login='', passwd=''):
        super(MikroNas, self).__init__(hosts, users)
        self.__hw_nas = Core(address)
        self.__hw_nas.login(login, passwd)

    def _hw_get_all_hosts(self):
        lists = {item['list']:item for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/address-list/print','?dynamic=false','?disabled=false')))}
        print lists.keys()

    def periodic_proc(self):
        self._hw_get_all_hosts()
