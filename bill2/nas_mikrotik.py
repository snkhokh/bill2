__author__ = 'sn355_000'
from bill2.nas import Nas
from util.RosAPI import Core
from util import net
from threading import Lock


class MikroNas(Nas):
    def __init__(self, hosts=None, address='', login='', passwd=''):
        super(MikroNas, self).__init__(hosts)
        self.__hw_nas = Core(address)
        self.__hw_nas.login(login, passwd)
        self.__hw_lock = Lock()
        self.__hosts_state = self.__get_hw_nas_state

    @property
    def __get_hw_nas_state(self):
        state = dict()

        def add_key_to_host(ip, i):
            if not ip in state:
                state[ip] = dict()
            state[ip].update(i)

        def add_alist_to_host(ip, alist, alist_id):
            i = {alist[:4]: {'id': alist_id, 'val': int(alist[4:])}} if alist[:4] in ('b_fl', 'b_up', 'b_dw') \
                else {alist: {'id': alist_id}} if alist in ('b_reg', 'b_act') \
                else {}
            if i:
                if not ip in state:
                    state[ip] = dict()
                state[ip].update(i)

        for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/address-list/print',
                                                                       '?dynamic=false', '?disabled=false'))):
            add_alist_to_host(item['address'], item['list'], item['.id'])
        for item in self.__hw_nas.response_handler(self.__hw_nas.talk(('/ip/firewall/filter/print', '?chain=count'))):
            if 'src-address' in item:
                add_key_to_host(item['src-address'], {'cnt_up': {'id': item['.id'], 'val': int(item['bytes'])}})
            elif 'dst-address' in item:
                add_key_to_host(item['dst-address'], {'cnt_dw': {'id': item['.id'], 'val': int(item['bytes'])}})
        return state

    def _get_statis_hosts_set(self):
        return {h for h in self.__hosts_state.keys() if 'cnt_dw' in self.__hosts_state[h]}

    def _get_reg_hosts_set(self):
        return {h for h in self.__hosts_state.keys() if 'b_reg' in self.__hosts_state[h]}

    def _get_act_hosts_set(self):
        return {h for h in self.__hosts_state.keys() if 'b_act' in self.__hosts_state[h]}

    def _set_statis_hosts(self, hosts_to_set):
        if hosts_to_set:
            with self.__hw_lock:
                for ip in hosts_to_set:
                    if not ip in self.__hosts_state:
                        self.__hosts_state[ip] = dict()
                    (reply, param) = self.__hw_nas.talk(('/ip/firewall/filter/add', '=chain=count',
                                                        '=dst-address=' + ip, '=action=return'))[-1:][0]
                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            self.__hosts_state[ip].update({'cnt_dw': {'id': item_id, 'val': 0}})
                    (reply, param) = self.__hw_nas.talk(('/ip/firewall/filter/add', '=chain=count',
                                                         '=src-address=' + ip, '=action=return'))[-1:][0]
                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            self.__hosts_state[ip].update({'cnt_up': {'id': item_id, 'val': 0}})

    def _unset_statis_hosts(self, hosts_to_unset):
        ids = list()
        with self.__hw_lock:
            for ip in hosts_to_unset:
                if ip in self.__hosts_state:
                    if 'cnt_up' in self.__hosts_state[ip]:
                        ids.append(self.__hosts_state[ip]['cnt_up']['id'])
                        self.__hosts_state[ip].pop('cnt_up')
                    if 'cnt_dw' in self.__hosts_state[ip]:
                        ids.append(self.__hosts_state[ip]['cnt_dw']['id'])
                        self.__hosts_state[ip].pop('cnt_dw')
            if ids:
                self.__hw_nas.talk(('/ip/firewall/filter/remove', '=.id=' + ','.join(ids)))

    def _set_addr_list_for_hosts(self, hosts_to_set, name):
        if hosts_to_set:
            with self.__hw_lock:
                for ip in hosts_to_set:
                    (reply, param) = self.__hw_nas.talk(('/ip/firewall/address-list/add', '=list=%s' % name,
                                                        '=address=%s' % ip, '=comment=bill2_dyn'))[-1:][0]
                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            if not ip in self.__hosts_state:
                                self.__hosts_state[ip] = dict()
                            self.__hosts_state[ip].update({name: {'id': item_id}})

    def _unset_addr_list_for_hosts(self, hosts_to_unset, name):
        ids = list()
        with self.__hw_lock:
            for ip in hosts_to_unset:
                if ip in self.__hosts_state:
                    if name in self.__hosts_state[ip]:
                        ids.append(self.__hosts_state[ip][name]['id'])
                        self.__hosts_state[ip].pop(name)
            if ids:
                self.__hw_nas.talk(('/ip/firewall/address-list/remove', '=.id=' + ','.join(ids)))

    def _reg_hosts(self, hosts_to_reg):
        self._set_addr_list_for_hosts(hosts_to_reg, 'b_reg')

    def _unreg_hosts(self, hosts_to_unreg):
        self._unset_addr_list_for_hosts(hosts_to_unreg, 'b_reg')

    def _act_hosts(self, hosts_to_set):
        self._set_addr_list_for_hosts(hosts_to_set, 'b_act')

    def _down_hosts(self, hosts_to_down):
        self._unset_addr_list_for_hosts(hosts_to_down, 'b_act')
