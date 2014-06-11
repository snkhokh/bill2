__author__ = 'sn355_000'
from bill2.nas import Nas
from util.RosAPI import Core
from threading import Lock


class MikroNas(Nas):

    def __init__(self, hosts=None, users=None, address='', login='', passwd=''):
        super(MikroNas, self).__init__(hosts, users)
        self.__hw_nas = Core(address)
        self.__hw_nas.login(login, passwd)
        self.__hw_lock = Lock()
        with self.__hw_lock:
            self.__hosts_state = self.__get_hw_nas_state()

    def __get_hw_nas_state(self):

        state = dict()
        def add_key_to_host(ip, item):
            if not ip in state: state[ip] = dict()
            state[ip].update(item)

        def add_alist_to_host(ip,alist,alist_id):
            i = {alist[:4]:{'id':alist_id,'val':int(alist[4:])}} if alist[:4] in ('b_fl','b_up','b_dw')\
                else {alist:{'id':alist_id}} if alist in ('b_reg','b_act')\
                else {}
            if i:
                if not ip in state: state[ip] = dict()
                state[ip].update(i)

        #get all address-lists
        for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/address-list/print', '?dynamic=false', '?disabled=false'))):
            add_alist_to_host(item['address'],item['list'],item['.id'])

        #get all count chains
        for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/filter/print', '?chain=count'))):
            if 'src-address' in item:
                add_key_to_host(item['src-address'],{'cnt_up_id': item['.id']})
                add_key_to_host(item['src-address'],{'cnt_up': int(item['bytes'])})
            elif 'dst-address' in item:
                add_key_to_host(item['dst-address'],{'cnt_dw_id': item['.id']})
                add_key_to_host(item['dst-address'],{'cnt_dw': int(item['bytes'])})

        return state


    def _hw_get_all_hosts(self):
        print self.__get_hw_nas_state()

    def periodic_proc(self):
        self._hw_get_all_hosts()

    def __create_count_pare(self, host_id):
        host = self.get_host(host_id)
        ip = self.ip_ntos(host.ip, prefix=host.lprefix)
        with self.__hw_lock:
            if not self.__hosts_state.has_key(ip): self.__hosts_state[ip]= dict()
            if not self.__hosts_state[ip].has_key('cnt_dw_id'):
                (ch_id,) = self.__hw_nas.response_handler(self.__hw_nas.talk('/ip/firewall/filter/add', '=chain=count', '=dst-address=' + ip, '=action=return'))
                if ch_id:
                    self.__hosts_state[ip]['cnt_dw_id'] = ch_id
                    self.__hosts_state[ip]['cnt_dw'] = 0
            if not self.__hosts_state[ip].has_key('cnt_up_id'):
                (ch_id,) = self.__hw_nas.response_handler(self.__hw_nas.talk('/ip/firewall/filter/add', '=chain=count', '=src-address=' + ip, '=action=return'))
                if ch_id:
                    self.__hosts_state[ip]['cnt_up_id'] = ch_id
                    self.__hosts_state[ip]['cnt_up'] = 0

    def __rm_count_pare(self, host_id):
        host = self.get_host(host_id)
        ip = self.ip_ntos(host.ip, prefix=host.lprefix)
        with self.__hw_lock:
            if not ip in self.__hosts_state:
                return
            ids = list()
            if 'cnt up_id' in self.__hosts_state[ip]:
                ids.append(self.__hosts_state[ip]['cnt up_id'])
            if 'cnt dw_id' in self.__hosts_state[ip]:
                ids.append(self.__hosts_state[ip]['cnt dw_id'])
            if ids:
                self.__hw_nas.talk('/ip/firewall/filter/remove', '=.id='+','.join(ids))
