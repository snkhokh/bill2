__author__ = 'sn'

class TP:
    def __init__(self,id,name,maxin=0,maxout=0):
        self.__id = id
        self.__name = name
        self.__maxin = maxin
        self.__maxout = maxout


    def test(self,host):

        """

        :type host: Host
        """
        if host.count_in > self.__maxin: print 'MAXIN!'
        if host.count_out > self.__maxout: print 'MAXOUT!'



class TrafPlans:
    def __init__(self):
        self.__plans = dict()

