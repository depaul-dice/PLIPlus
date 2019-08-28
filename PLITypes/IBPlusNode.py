#!/usr/bin/python

from PLITypes import Constants

#Non-Leaf node
class IBPlusNLNode():
    def __init__(self):
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_NL_ENTRY)]
        self.max = [0.0 for x in range(0, Constants.MAX_NUM_NL_ENTRY + 1)]
        self.pointer = [None for x in range(0, Constants.MAX_NUM_NL_ENTRY + 1)]
        self.level = 0
        self.length = 0

    def isLeaf(self):
        return Constants.FUNC_FALSE

#Leaf node
class IBPlusLNode():
    def __init__(self):
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_L_ENTRY)]
        self.data = [[None for x in range(0, Constants.NUM_ROW_PER_BUCKET)] for y in range(0, Constants.MAX_NUM_L_ENTRY)] #each value points to a row/tuple
        self.count = [0 for y in range(0, Constants.MAX_NUM_L_ENTRY)]
        self.sibling = None
        self.level = 0
        self.length = 0

    def isLeaf(self):
        return Constants.FUNC_TRUE

def newIBPlusNode(isLeaf):
    if(isLeaf):
        return IBPlusLNode()
    else:
        return IBPlusNLNode()

#Non-Leaf node buffer
class IBPlusNLBuffer():
    def __init__(self):
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_NL_ENTRY + 1)]
        self.max = [0.0 for x in range(0, Constants.MAX_NUM_NL_ENTRY + 1 + 1)]
        self.pointer = [None for x in range(0, Constants.MAX_NUM_NL_ENTRY + 1 + 1)]
        self.level = 0
        self.length = 0

    def isLeaf(self):
        return Constants.FUNC_FALSE

#Leaf node buffer
class IBPlusLBuffer():
    def __init__(self):
        self.interval = [[0.0 for x in range(0, Constants.HIGH + 1)] for y in range(0, Constants.MAX_NUM_L_ENTRY + 1)]
        self.data = [[None for x in range(0, Constants.NUM_ROW_PER_BUCKET)] for y in range(0, Constants.MAX_NUM_L_ENTRY + 1)] #each value points to a row/tuple
        self.count = [0 for y in range(0, Constants.MAX_NUM_L_ENTRY)]
        self.sibling = None
        self.level = 0
        self.length = 0

    def isLeaf(self):
        return Constants.FUNC_TRUE