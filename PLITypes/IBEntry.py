#!/usr/bin/python

import Constants

class IBEntry():
    def __init__(self):
        self.interval = [0.0 for x in range(0, Constants.HIGH + 1)]
        self.max = 0.0
        self.bucketID = 0

    def __init__(self, _interval=None, _max=0.0, _bucketID=0):
        self.interval = [0.0 for x in range(0, Constants.HIGH + 1)]
        self.max = _max
        self.bucketID = _bucketID