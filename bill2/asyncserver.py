__author__ = 'sn355_000'

import asyncore, asynchat, socket, os, sys, traceback

from util.helpers import getLogger

logSys = getLogger(__name__)

class RequestHandler(asynchat.async_chat):
    END_STR = '\r\n'
    def __init__(self, sock=None, map=None):
        asynchat.async_chat.__init__(self, sock, map)
        self.__buffer = []
        self.set_terminator(RequestHandler.END_STR)

    def collect_incoming_data(self, data):
        logSys.debug('Received data: %s',str(data))
        self.__buffer.append(data)

    def found_terminator(self):
        msg = ''.join(self.__buffer)
        self.push(msg+'RET'+RequestHandler.END_STR)


class AsyncServer(asyncore.dispatcher):
    def __init__(self, sock=None, map=None):
        asyncore.dispatcher.__init__(self)