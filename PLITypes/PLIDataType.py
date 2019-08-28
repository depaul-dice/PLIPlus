#!/usr/bin/python

import Constants

class clusteredData():
    def __init__(self):
        self.interval = [[Constants.MAX_DISTANCE, Constants.MIN_DISTANCE] for y in range(0, Constants.MAX_SIZE_CLUSTERED)]
        self.bucketID = [0 for x in range(0, Constants.MAX_SIZE_CLUSTERED)]

class overflowData():
    def __init__(self):
        self.pointer = 0
        self.max = Constants.MAX_DISTANCE
        self.min = Constants.MIN_DISTANCE