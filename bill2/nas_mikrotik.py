__author__ = 'sn355_000'
from bill2.nas import Nas
from util.RosAPI import Core,MikrotikConnectError
from threading import Lock


class MikroNas(Nas):
    def __init__(self, hosts=None, address='', login='', passwd=''):
        super(MikroNas, self).__init__(hosts)
        self.__addr = address
        self.__login = login
        self.__pass = passwd
        self.__hw_lock = Lock()
        self.__hosts_state = None

    def connect(self):
        self.__hw_nas = Core(self.__addr)
        if self.__hw_nas.ok:
            self.__hw_nas.login(self.__login, self.__pass)
        if not self.__hw_nas.connected:
            raise MikrotikConnectError
        self.__hosts_state = self.__get_hw_nas_state

    def __hw_nas_safe_talk(self, words):
        try:
            return self.__hw_nas.talk(words)
        except MikrotikConnectError:
            del self.__hw_nas
            self.connect()
            return self.__hw_nas.talk(words)


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

    def _get_hosts_state(self):
        """
        :rtype : dict
        """
        return {h: (True if 'b_act' in self.__hosts_state[h] else False,
                    self.__hosts_state[h]['b_up']['val'] if 'b_up' in self.__hosts_state[h] else None,
                    self.__hosts_state[h]['b_dw']['val'] if 'b_dw' in self.__hosts_state[h] else None,
                    self.__hosts_state[h]['b_fl']['val'] if 'b_fl' in self.__hosts_state[h] else None,)
                for h in self.__hosts_state.keys() if 'b_reg' in self.__hosts_state[h]}


    def _set_addr_list_for_hosts(self, hosts_to_set, name):
        if hosts_to_set:
            with self.__hw_lock:
                for ip in hosts_to_set:
                    (reply, param) = self.__hw_nas_safe_talk(('/ip/firewall/address-list/add', '=list=%s' % name,
                                                         '=address=%s' % ip, '=comment=bill2_dyn'))[-1:][0]

                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            if not ip in self.__hosts_state:
                                self.__hosts_state[ip] = dict()
                            self.__hosts_state[ip].update({name: {'id': item_id}})

    def _hw_set_addr_list_for_host(self, ip, name):
        with self.__hw_lock:
            if not ip in self.__hosts_state:
                self.__hosts_state[ip] = dict()
            if not name in self.__hosts_state[ip]:
                (reply, param) = self.__hw_nas_safe_talk(('/ip/firewall/address-list/add', '=list=%s' % name,
                                                     '=address=%s' % ip, '=comment=bill2_dyn'))[-1:][0]
                if reply == '!done':
                    item_id = param.get('=ret')
                    if item_id:
                        self.__hosts_state[ip].update({name: {'id': item_id}})

    def _hw_set_val_addr_list_for_host(self, ip, name, val):
        with self.__hw_lock:
            if not ip in self.__hosts_state:
                self.__hosts_state[ip] = dict()
            if not name in self.__hosts_state[ip]:
                (reply, param) = self.__hw_nas_safe_talk(('/ip/firewall/address-list/add', '=list=%s%s' % (name, val),
                                                     '=address=%s' % ip, '=comment=bill2_dyn'))[-1:][0]
                if reply == '!done':
                    item_id = param.get('=ret')
                    if item_id:
                        self.__hosts_state[ip].update({name: {'id': item_id, 'val': val}})

    def _unset_addr_list_for_hosts(self, hosts_to_unset, name):
        ids = list()
        with self.__hw_lock:
            for ip in hosts_to_unset:
                if ip in self.__hosts_state:
                    if name in self.__hosts_state[ip]:
                        ids.append(self.__hosts_state[ip][name]['id'])
                        self.__hosts_state[ip].pop(name)
            if ids:
                self.__hw_nas_safe_talk(('/ip/firewall/address-list/remove', '=.id=' + ','.join(ids)))

    def _hw_unset_addr_list_for_host(self, ip, name):
        with self.__hw_lock:
            if ip in self.__hosts_state and name in self.__hosts_state[ip]:
                self.__hw_nas_safe_talk(('/ip/firewall/address-list/remove', '=.id=' + self.__hosts_state[ip][name]['id']))
                del self.__hosts_state[ip][name]

    def _unreg_host(self, ip):
        if 'b_act' in self.__hosts_state[ip]:
            self._hw_unset_addr_list_for_host(ip, 'b_act')
        if 'b_reg' in self.__hosts_state[ip]:
            self._hw_unset_addr_list_for_host(ip, 'b_reg')
        if 'b_fl' in self.__hosts_state[ip]:
            self._hw_unset_addr_list_for_host(ip, 'b_fl')
        if 'b_up' in self.__hosts_state[ip]:
            self._hw_unset_addr_list_for_host(ip, 'b_up')
        if 'b_dw' in self.__hosts_state[ip]:
            self._hw_unset_addr_list_for_host(ip, 'b_dw')
        if 'cnt_up' in self.__hosts_state[ip]:
            self._hw_unset_stats_for_host(ip)

    def _set_host_state(self, ip, state):
        self._hw_set_addr_list_for_host(ip,'b_reg')
        if state[0]:
            self._hw_set_addr_list_for_host(ip, 'b_act')
        else:
            self._hw_unset_addr_list_for_host(ip, 'b_act')
        # upload speed
        if state[1]:
            if 'b_up' in self.__hosts_state[ip] and not self.__hosts_state[ip]['b_up']['val'] == state[1]:
                self._hw_unset_addr_list_for_host(ip, 'b_up')
            self._hw_set_val_addr_list_for_host(ip, 'b_up', state[1])
        else:
            self._hw_unset_addr_list_for_host(ip, 'b_up')
        # download speed
        if state[2]:
            if 'b_dw' in self.__hosts_state[ip] and not self.__hosts_state[ip]['b_dw']['val'] == state[1]:
                self._hw_unset_addr_list_for_host(ip, 'b_dw')
            self._hw_set_val_addr_list_for_host(ip, 'b_dw', state[2])
        else:
            self._hw_unset_addr_list_for_host(ip, 'b_dw')
        # filter set
        if state[3]:
            if 'b_fl' in self.__hosts_state[ip] and not self.__hosts_state[ip]['b_fl']['val'] == state[1]:
                self._hw_unset_addr_list_for_host(ip, 'b_fl')
            self._hw_set_val_addr_list_for_host(ip, 'b_fl', state[3])
        else:
            self._hw_unset_addr_list_for_host(ip, 'b_fl')
        ###########################################################################################################


        ###########################################################################################################
        # Stats manage
    def _hw_get_hosts_stats_set(self):
        return {h for h in self.__hosts_state.keys() if 'cnt_dw' in self.__hosts_state[h]}

    def _get_hw_stats(self):
        return ((host, {'up': int(self.__hosts_state[host]['cnt_up']['val']),
                        'dw': int(self.__hosts_state[host]['cnt_dw']['val'])}) for host in self.__hosts_state
                if 'cnt_up' in self.__hosts_state[host] and 'cnt_dw' in self.__hosts_state[host])
    def _hw_update_stats(self):
        for item in self.__hw_nas.response_handler(self.__hw_nas_safe_talk(('/ip/firewall/filter/print', '?chain=count'))):
            try:
                if 'src-address' in item:
                    self.__hosts_state[item['src-address']]['cnt_up']['val'] = item['bytes']
                elif 'dst-address' in item:
                    self.__hosts_state[item['dst-address']]['cnt_dw']['val'] = item['bytes']
            except KeyError:
                print 'Nas synchronisation error!'

    def _hw_set_stats_for_hosts(self, hosts_to_set):
        if hosts_to_set:
            with self.__hw_lock:
                for ip in hosts_to_set:
                    if not ip in self.__hosts_state:
                        self.__hosts_state[ip] = dict()
                    (reply, param) = self.__hw_nas_safe_talk(('/ip/firewall/filter/add', '=chain=count',
                                                         '=dst-address=' + ip, '=action=return'))[-1:][0]
                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            self.__hosts_state[ip].update({'cnt_dw': {'id': item_id, 'val': 0}})
                    (reply, param) = self.__hw_nas_safe_talk(('/ip/firewall/filter/add', '=chain=count',
                                                         '=src-address=' + ip, '=action=return'))[-1:][0]
                    if reply == '!done':
                        item_id = param.get('=ret')
                        if item_id:
                            self.__hosts_state[ip].update({'cnt_up': {'id': item_id, 'val': 0}})

    def _hw_unset_stats_for_hosts(self, hosts_to_unset):
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
                self.__hw_nas_safe_talk(('/ip/firewall/filter/remove', '=.id=' + ','.join(ids)))

    def _hw_unset_stats_for_host(self, ip):
        ids = list()
        with self.__hw_lock:
            if ip in self.__hosts_state:
                if 'cnt_up' in self.__hosts_state[ip]:
                    ids.append(self.__hosts_state[ip]['cnt_up']['id'])
                    self.__hosts_state[ip].pop('cnt_up')
                if 'cnt_dw' in self.__hosts_state[ip]:
                    ids.append(self.__hosts_state[ip]['cnt_dw']['id'])
                    self.__hosts_state[ip].pop('cnt_dw')
            if ids:
                self.__hw_nas_safe_talk(('/ip/firewall/filter/remove', '=.id=' + ','.join(ids)))
###############################################################################################


