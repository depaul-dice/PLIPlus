#!/usr/bin/python
from PLITypes import Constants
class ListBuckets():
    def __init__(self):
        self.results = []

class ListTuples():
    def __init__(self):
        self.results = []

class recordMax():
    def __init__(self):
        self.curMax = 0.0
        self.newMax = 0.0

    def __init__(self, _curMax=0.0, _newMax=0.0):
        self.curMax = _curMax
        self.newMax = _newMax

class recordRec():
    def __init__(self):
        self.selEntry = None
        self.newNode = None

    def __init__(self, _entry=None, _node=None):
        self.selEntry = _entry
        self.newNode = _node

class Tuple():
    def __init__(self):
        self.key = 0.0
        self.data = []

class Bucket():
    def __init__(self):
        self.bucketID = 0
        self.tuples = []

class Evaluation():
    def __init__(self):
        self.scannedEntries = 0
        self.returnBuckets = 0
        self.DBMIN = Constants.MAX_DISTANCE
        self.DBMAX = Constants.MIN_DISTANCE
        self.IntervalLow = 0.0
        self.IntervalHigh = 0.0
        self.totalBuckets = 0
        #others
        #self.insertionTime = 0.0
        #self.searchingTime = 0.0
        #self.start = 0.0
        #self.end = 0.0

    def countSE(self, num=1):
        self.scannedEntries += num
        return Constants.SUCCESS

    def countReturnBuckets(self, num=1):
        self.returnBuckets += num
        return Constants.SUCCESS

    def countTotalBuckets(self, num=1):
        self.totalBuckets += num
        return Constants.SUCCESS

    def setInterval(self, interval):
        self.IntervalLow = interval[Constants.LOW]
        self.IntervalHigh = interval[Constants.HIGH]
        return Constants.SUCCESS

    def setDBInterval(self, interval):
        if(self.DBMAX < interval[Constants.HIGH]):
            self.DBMAX = interval[Constants.HIGH]
        if(self.DBMIN > interval[Constants.LOW]):
            self.DBMIN = interval[Constants.LOW]

    def getSE(self):
        return self.scannedEntries

    def getReturnBuckets(self):
        return self.returnBuckets

    def printEvalInfo(self):
        print "===============<Evaluation Information>=================="
        print "Total number of buckets in database: " + str(self.totalBuckets)
        print "MIN value: " + str(self.DBMIN) + "; MAX value: " + str(self.DBMAX)
        print "Interval: [" + str(self.IntervalLow) + ", " + str(self.IntervalHigh) + "]"
        print "Scanned Buckets: " + str(self.scannedEntries)
        print "Results: " + str(self.returnBuckets) + " buckets"
        print "==========================<End>=========================="
        return Constants.SUCCESS

    def printEvalInfoToFile(self, filename):
        fout = open(filename, "a+")
        strData = "===============<Evaluation Information>==================" + "\n"
        strData += "Total number of buckets in database: " + str(self.totalBuckets) + "\n"
        strData += "MIN value: " + str(self.DBMIN) + "; MAX value: " + str(self.DBMAX) + "\n"
        strData += "Interval: [" + str(self.IntervalLow) + ", " + str(self.IntervalHigh) + "]" + "\n"
        strData += "Scanned Buckets: " + str(self.scannedEntries) + "\n"
        strData += "Results: " + str(self.returnBuckets) + " buckets" + "\n"
        strData += "==========================<End>==========================" + "\n"
        fout.write(strData)
        fout.close()
        return Constants.SUCCESS

class nodePointer:
    def __init__(self):
        self.pointer = None
        self.index = -1

class Stack:
    def __init__(self):
        self.items = []

    def isEmpty(self):
        return self.items == []

    def push(self, item):
        self.items.append(item)

    def pop(self):
        return self.items.pop()

    def peek(self):
        return self.items[len(self.items) - 1]

    def size(self):
        return len(self.items)

    def clear(self):
        while (not self.isEmpty()):
            self.items.pop()