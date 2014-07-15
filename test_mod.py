__author__ = 'sn'

class cl_a:
    def __init__(self):
        self.__a1 = 10
        self.__a2 = 20

    def pr(self):
        print self.__a1
        print self.__a2
        self.__q()

    def __q(self):
        print 'q from a'

class cl_b(cl_a):
    def __init__(self):
        cl_a.__init__(self)
        self.__a2 = 33

    def pr(self):
        print 'q from b'


