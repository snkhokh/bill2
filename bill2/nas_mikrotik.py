__author__ = 'sn355_000'
from bill2.nas import Nas
from util.RosAPI import Core


class MikroNas(Nas):
    def __init__(self, hosts=None, users=None, address='', login='', passwd=''):
        super(MikroNas, self).__init__(hosts, users)
        self.__hw_nas = Core(address)
        self.__hw_nas.login(login, passwd)

    def _hw_get_all_hosts(self):
        lists = {}
        for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/address-list/print','?dynamic=false','?disabled=false'))):
            if item['list'] in lists: lists[item['list']][item['address']] = item['.id']
            else: lists[item['list']] = {item['address']:item['.id']}
        print lists

    def periodic_proc(self):
        self._hw_get_all_hosts()
