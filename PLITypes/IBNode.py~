#!/usr/bin/python

import Constants
class IBNode():
    def __init__(self):
        self.level = 0
        self.length = 0
        self.isLeaf = True
        #Entries
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_ENTRY)]
        self.max = [0.0 for x in range(0, Constants.MAX_NUM_ENTRY + 1)]
        self.bucketID = [0 for x in range(0, Constants.MAX_NUM_ENTRY)]
        #Pointers
        self.pointer = [None for x in range(0, Constants.MAX_NUM_ENTRY + 1)] #n+1 children (pointers)

    def __init__(self, _level=0, _length=0, _max=0, _type=True):
        self.level = _level
        self.length = _length
        self.isLeaf = _type
        #Entries
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_ENTRY)]
        self.max = [0.0 for x in range(0, Constants.MAX_NUM_ENTRY + 1)]
        self.bucketID = [-1 for x in range(0, Constants.MAX_NUM_ENTRY)]
        #Pointers
        self.pointer = [None for x in range(0, Constants.MAX_NUM_ENTRY + 1)] #n+1 children (pointers)

class IBNodeBuffer():
    def __init__(self):
        self.level = 0
        self.length = 0
        self.isLeaf = True
        #Entries
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_ENTRY + 1)]
        self.max = [0.0 for x in range(0, Constants.MAX_NUM_ENTRY + 1 + 1)]
        self.bucketID = [0 for x in range(0, Constants.MAX_NUM_ENTRY + 1 + 1)]
        #Pointers
        self.pointer = [None for x in range(0, Constants.MAX_NUM_ENTRY + 1 + 1)] #n+1 children (pointers)

