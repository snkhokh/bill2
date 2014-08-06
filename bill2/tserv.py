__author__ = 'sn355_000'

import SocketServer
import time
import threading

from telnetsrv.threaded import TelnetHandler, command

from bill2.soft_worker import SoftWorker, Hosts, Users

class CommandHandler(object, TelnetHandler):
    WELCOME = "Billing core server"

    def __init__(self, request, client_address, server):
        self.__close_flag = False
        #: :type: SoftWorker
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

    @command(['state'])
    def command_get_state(self, params):
        '''<>
        Print state of worker object.

        '''
        a = self.__worker.prepare_state()
        self.writeresponse(str(a))


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
        SocketServer.ThreadingTCPServer.__init__(self, ('0.0.0.0', 23), CommandHandler)

    def shutdown(self):
        SocketServer.ThreadingTCPServer.shutdown(self)
        with self.__handlers_lock:
            for h in self.__handlers_list:
                h.close()

    def append_handler(self,handler):
        with self.__handlers_lock:
            self.__handlers_list.append(handler)

    def remove_handler(self, handler):
        with self.__handlers_lock:
            self.__handlers_list.remove(handler)

    def msg_to_all(self,txt):
        with self.__handlers_lock:
            for h in self.__handlers_list:
                assert isinstance(h, CommandHandler)
                h.writemessage(txt)
