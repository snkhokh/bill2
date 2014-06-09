__author__ = 'sn355_000'

class Command():
    def __init__(self,cmd,**karg):
        self.__cmd = cmd
        self.__params = karg

    @property
    def cmd(self):
        return self.__cmd

    @property
    def hostid(self):
        if 'hostid' in self.__params: return self.__params['hostid']
        else: return None

    @property
    def uid(self):
        if 'uid' in self.__params: return self.__params['uid']
        else: return None

    @property
    def params(self):
        return self.__params




