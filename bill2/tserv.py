__author__ = 'sn355_000'

import SocketServer
import time
import threading

from telnetsrv.threaded import TelnetHandler, command
import bill2.soft_worker
from bill2.version import version
from util.helpers import getLogger

logSys = getLogger(__name__)


class CommandHandler(object, TelnetHandler):
    WELCOME = "Bill2 v %s core telnet server" % version

    def __init__(self, request, client_address, server):
        self.__close_flag = False
        assert isinstance(server.worker, bill2.soft_worker.SoftWorker)
        self.__worker = server.worker
        self.__nas = server.nas
        TelnetHandler.__init__(self, request, client_address, server)

    def session_start(self):
        self.server.append_handler(self)

    def session_end(self):
        self.server.remove_handler(self)

    def close(self):
        self.__close_flag = True

    def getc(self, block=True):
        if not block and not len(self.cookedq) and not self.__close_flag:
            return ''
        while not self.__close_flag and not len(self.cookedq):
            time.sleep(0.05)
        if self.__close_flag:
            return chr(4)
        self.IQUEUELOCK.acquire()
        ret = self.cookedq[0]
        self.cookedq = self.cookedq[1:]
        self.IQUEUELOCK.release()
        return ret

    def inputcooker(self):
        try:
            super(CommandHandler, self).inputcooker()
        except:
            pass

    @command('print')
    def command_print(self, params):
        '''
        :type params: list
        :return:
        '''
        if not len(params):
            self.writeerror('arguments need!')
            return
        name = params[0]
        if name == 'trafplans':
            for s in self.__worker.fget_tps(None):
                self.writeresponse(s)
        elif name == 'state':
            self.writeresponse(str(self.__worker.prepare_state()))



class TnetServer(SocketServer.ThreadingTCPServer):
    allow_reuse_address = True

    def __init__(self, sw_worker, hw_worker):
        '''
        :type sw_worker: SoftWorker
        '''
        self.__handlers_lock = threading.Lock()
        self.__handlers_list = []
        self.worker = sw_worker
        self.nas = hw_worker
        sw_worker.set_tnserver(self)
        logSys.debug('Create socket server')
        SocketServer.ThreadingTCPServer.__init__(self, ('0.0.0.0', 23), CommandHandler)

    def shutdown(self):
        logSys.debug('Shutdown socket server')
        SocketServer.ThreadingTCPServer.shutdown(self)
        with self.__handlers_lock:
            for h in self.__handlers_list:
                h.close()

    def append_handler(self, handler):
        with self.__handlers_lock:
            self.__handlers_list.append(handler)

    def remove_handler(self, handler):
        with self.__handlers_lock:
            self.__handlers_list.remove(handler)

    def msg_to_all(self, txt):
        with self.__handlers_lock:
            for h in self.__handlers_list:
                assert isinstance(h, CommandHandler)
                h.writemessage(txt)
