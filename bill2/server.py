__author__ = 'sn'
import signal
import os
import sys
import errno

from bill2.nas_mikrotik import MikroNas
from bill2.user import Users
from bill2.host import Hosts
from bill2.commands import Command
from bill2.config import nases
from bill2.version import version
from bill2.soft_worker import SoftWorker
import threading
import logging
import logging.handlers
from time import sleep

from util.helpers import getLogger

logSys = getLogger(__name__)


class Server:
    def __init__(self, daemon=False):
        self.__loggingLock = threading.Lock()
        self.__lock = threading.RLock()
        self.__daemon = daemon
        self.__logLevel = None
        self.__logTarget = None
        # Set logging level
        self.setLogLevel("DEBUG")
        # self.setLogTarget("STDOUT")
        self.setLogTarget("SYSLOG")
        self.__soft_worker = SoftWorker()
        self.__nas1 = MikroNas(hosts=self.__soft_worker, address=nases['m1']['address'], login=nases['m1']['login'],
                               passwd=nases['m1']['passwd'])

    def __sigTERMhandler(self, signum, frame):
        logSys.debug("Caught signal %d. Exiting" % signum)
        self.quit()

    def start(self, sock, pidfile):
        logSys.info("Starting Bill2 v %s", version)
        # Ensure unhandled exceptions are logged
        # sys.excepthook = excepthook

        # First set the mask to only allow access to owner
        os.umask(0077)
        if self.__daemon:  # pragma: no cover
            logSys.info("Starting in daemon mode")
            ret = self.__createDaemon()
            if ret:
                logSys.info("Daemon started")
            else:
                logSys.error("Could not create daemon")
                raise ServerInitializationError("Could not create daemon")
        # Install signal handlers
        signal.signal(signal.SIGTERM, self.__sigTERMhandler)
        signal.signal(signal.SIGINT, self.__sigTERMhandler)
        # Creates a PID file.
        logSys.debug("Creating PID file %s" % pidfile)
        try:
            pid_fd = os.open(pidfile, os.O_CREAT | os.O_WRONLY | os.O_EXCL)
        except OSError as e:
            logSys.error("Unable to create PID file: %s" % e)
            if e.errno == errno.EEXIST:
                pid = self.check_pid(pidfile)
                if pid:
                    raise ProcessRunningException('process already running in %s as pid %s' % (pidfile, pid));
                else:
                    os.remove(pidfile)
                    logSys.warn('removed staled lockfile %s',pidfile)
                    pid_fd = os.open(pidfile, os.O_CREAT | os.O_WRONLY | os.O_EXCL)
            else:
                raise
        os.write(pid_fd, "%s\n" % os.getpid())
        os.close(pid_fd)

        # Start the communication

        logSys.debug("Starting communication")
        try:
            self.__soft_worker.start()
            self.__nas1.start()
            #
            while self.__soft_worker.isAlive() or self.__nas1.isAlive():
                sleep(1)

            logSys.info("Exiting Bill2")

        except AsyncServerException, e:
            logSys.error("Could not start server: %s", e)
        # Removes the PID file.
        try:
            logSys.debug("Remove PID file %s" % pidfile)
            os.remove(pidfile)
        except OSError, e:
            logSys.error("Unable to remove PID file: %s" % e)


    def check_pid(self,pidfile):
        with open(pidfile, 'r') as f:
            try:
                pidstr = f.read()
                pid = int(pidstr)
            except ValueError:
                # not an integer
                logSys.debug("Pidfile not an integer: %s",pidstr)
                return False
            try:
                os.kill(pid, 0)
            except OSError:
                logSys.debug("can't deliver signal to %s",pid)
                return False
            else:
                return pid

    def quit(self):
        if threading.active_count:
            for t in threading.enumerate():
                if hasattr(t, 'cancel'):
                    t.cancel()
        self.__nas1.put_cmd(Command('stop'))
        self.__soft_worker.put_cmd(Command('stop'))
        # Only now shutdown the logging.
        try:
            self.__loggingLock.acquire()
            logging.shutdown()
        finally:
            self.__loggingLock.release()

    def setLogLevel(self, value):
        try:
            self.__loggingLock.acquire()
            getLogger("bill2").setLevel(
                getattr(logging, value.upper()))
        except AttributeError:
            raise ValueError("Invalid log level")
        else:
            self.__logLevel = value.upper()
        finally:
            self.__loggingLock.release()

    def setLogTarget(self, target):
        try:
            self.__loggingLock.acquire()
            # set a format which is simpler for console use
            formatter = logging.Formatter("%(asctime)s %(name)-24s[%(process)d]: %(levelname)-7s %(message)s")
            if target == "SYSLOG":
                # Syslog daemons already add date to the message.
                formatter = logging.Formatter("%(name)s[%(process)d]: %(levelname)s %(message)s")
                facility = logging.handlers.SysLogHandler.LOG_DAEMON
                hdlr = logging.handlers.SysLogHandler("/dev/log", facility=facility)
            elif target == "STDOUT":
                hdlr = logging.StreamHandler(sys.stdout)
            elif target == "STDERR":
                hdlr = logging.StreamHandler(sys.stderr)
            else:
                # Target should be a file
                try:
                    open(target, "a").close()
                    hdlr = logging.handlers.RotatingFileHandler(target)
                except IOError:
                    logSys.error("Unable to log to " + target)
                    logSys.info("Logging to previous target " + self.__logTarget)
                    return False
            # Removes previous handlers -- in reverse order since removeHandler
            # alter the list in-place and that can confuses the iterable
            logger = getLogger("bill2")
            for handler in logger.handlers[::-1]:
                # Remove the handler.
                logger.removeHandler(handler)
                # And try to close -- it might be closed already
                try:
                    handler.flush()
                    handler.close()
                except (ValueError, KeyError):  # pragma: no cover
                    # Is known to be thrown after logging was shutdown once
                    # with older Pythons -- seems to be safe to ignore there
                    # At least it was still failing on 2.6.2-0ubuntu1 (jaunty)
                    if (2, 6, 3) <= sys.version_info < (3,) or \
                                    (3, 2) <= sys.version_info:
                        raise
            # tell the handler to use this format
            hdlr.setFormatter(formatter)
            logger.addHandler(hdlr)
            # Does not display this message at startup.
            if not self.__logTarget is None:
                logSys.info("Changed logging target to %s for Fail2ban v%s" %
                            (target, 1))
            # Sets the logging target.
            self.__logTarget = target
            return True
        finally:
            self.__loggingLock.release()

    def __createDaemon(self):  # pragma: no cover
        signal.signal(signal.SIGHUP, signal.SIG_IGN)
        try:
            pid = os.fork()
        except OSError, e:
            return ((e.errno, e.strerror))  # ERROR (return a tuple)
        if pid == 0:  # The first child.
            os.setsid()
            try:
                pid = os.fork()  # Fork a second child.
            except OSError, e:
                return ((e.errno, e.strerror))  # ERROR (return a tuple)
            if (pid == 0):  # The second child.
                os.chdir("/")
            else:
                os._exit(0)  # Exit parent (the first child) of the second child.
        else:
            os._exit(0)  # Exit parent of the first child.
        try:
            maxfd = os.sysconf("SC_OPEN_MAX")
        except (AttributeError, ValueError):
            maxfd = 256  # default maximum
        if sys.version_info[0:3] == (3, 4, 0):  # pragma: no cover
            urandom_fd = os.open("/dev/urandom", os.O_RDONLY)
            for fd in range(0, maxfd):
                try:
                    if not os.path.sameopenfile(urandom_fd, fd):
                        os.close(fd)
                except OSError:  # ERROR (ignore)
                    pass
            os.close(urandom_fd)
        else:
            os.closerange(0, maxfd)
        # Redirect the standard file descriptors to /dev/null.
        os.open("/dev/null", os.O_RDONLY)  # standard input (0)
        os.open("/dev/null", os.O_RDWR)  # standard output (1)
        os.open("/dev/null", os.O_RDWR)  # standard error (2)
        return True


class AsyncServerException(Exception):
    pass


class ServerInitializationError(Exception):
    pass


class ProcessRunningException(BaseException):
    pass
