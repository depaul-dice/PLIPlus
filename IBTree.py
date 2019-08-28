#!/usr/bin/python

#import re
#import os, sys, time, datetime, glob, json
#import argparse

import PLITypes
#print dir(PLITypes)
from PLITypes.IBNode import *
from PLITypes import Constants
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from tools.assignInterval import assign
from tools.copyFromBuffer import *
from tools.copyToBuffer import *
import pdb
from itertools import islice

class IBTree():
    def __init__(self):
        self.rootNode = IBNode()
        self.evaluation = Evaluation()
        self.ibTreeMetaData = "ibTreeMD.dat"
        self.ibTreeDataBase = "ibPlusTreeDB.dat"
        # WriteMetaData
        self.nextPositionIndex = -1
        self.pointerStack = Stack()
    ##############<Insert>##############
    #1. Write the data into DB
    #2. Index the metadata in IB-Tree
    def insertBucket2(self, _interval, _bucketID, tuples):
        selEntry = IBEntry()
        newNode = IBNode()
        recMax = recordMax()
        recRec = recordRec()

        result = Constants.ERROR
        if(self.rootNode is None):
            return result
        self.evaluation.setDBInterval(_interval)
        self.evaluation.countTotalBuckets()
        result = self.insertBucketRec2(self.rootNode, _interval, _bucketID, selEntry, newNode, recRec, recMax, tuples)
        if (result == Constants.OVERFLOW):
            #Overflow in root -> increase the height of the tree
            newRoot = IBNode()
            newRoot.length = 1
            newRoot.level = self.rootNode.level + 1
            newRoot.isLeaf = False
            assign(newRoot.interval[0], recRec.selEntry.interval)
            newRoot.bucketID[0] = recRec.selEntry.bucketID
            if(newRoot.interval[0][Constants.HIGH] < recMax.curMax):
                newRoot.max[0] = recMax.curMax
            else:
                newRoot.max[0] = newRoot.interval[0][Constants.HIGH]
            newRoot.max[1] = recMax.newMax
            newRoot.pointer[0] = self.rootNode
            newRoot.pointer[1] = recRec.newNode
            self.rootNode = newRoot
            result = Constants.SUCCESS
        return result

    def insertBucketRec2(self, curNode, _interval, _bucketID, selEntry, newNode, recRec, recMax, tuples):
        result = Constants.ERROR
        if(curNode is None):
            return result
        if(curNode.isLeaf == True): #LEAF-NODE
            #Find a place to insert
            if (curNode.length >= Constants.MAX_NUM_ENTRY):
                #No more place, split at Leaf-level and return overflow
                recRec.selEntry = IBEntry()
                recRec.newNode = IBNode()
                recMax.newMax = Constants.MIN_DISTANCE
                recMax.curMax = Constants.MIN_DISTANCE
                #Find a place to split
                goodPlace = curNode.length
                for i in range(0, curNode.length):
                    if(curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                        goodPlace = i
                        break
                #1. Move data to buffer
                buffer = IBNodeBuffer()
                copyNodeToBuffer(buffer, curNode, _bucketID, _interval, -1.0, -1.0, None, goodPlace, Constants.MAX_NUM_ENTRY)
                #2. Copy from buffer
                copyNodeFromBuffer(buffer, curNode, 0, Constants.MAX_NUM_ENTRY / 2 - 1)
                copyEntryFromBuffer(buffer, recRec.selEntry, Constants.MAX_NUM_ENTRY / 2)
                copyNodeFromBuffer(buffer, recRec.newNode, Constants.MAX_NUM_ENTRY / 2 + 1, Constants.MAX_NUM_ENTRY)
                #3. Finalize
                recRec.newNode.isLeaf = True
                recRec.newNode.level = 0
                recRec.newNode.length = Constants.MAX_NUM_ENTRY/2
                for i in range(0, Constants.MAX_NUM_ENTRY/2):
                    if (recMax.newMax < recRec.newNode.max[i]):
                        recMax.newMax = recRec.newNode.max[i]
                curNode.length = Constants.MAX_NUM_ENTRY/2
                for i in range(0, Constants.MAX_NUM_ENTRY/2):
                    if (recMax.curMax < curNode.max[i]):
                        recMax.curMax = curNode.max[i]
                result = Constants.OVERFLOW #No more place return overflow

            else: ##curNode.length < Constants.MAX_NUM_ENTRY##
                goodPlace = curNode.length
                #Check if the current max value is the highest value
                isHighest = True
                for i in range(0, curNode.length + 1):
                    if(_interval[Constants.HIGH] < curNode.max[i]):
                        isHighest = False
                        break
                for i in range(0, curNode.length):
                    if (curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                        goodPlace = i
                        break
                #1. Move to buffer
                buffer = IBNodeBuffer()
                copyNodeToBuffer(buffer, curNode, _bucketID, _interval, -1.0, -1.0, None, goodPlace, Constants.MAX_NUM_ENTRY)
                #2. Copy from buffer
                copyNodeFromBuffer(buffer, curNode, 0, curNode.length)
                #3. Finialize
                curNode.length += 1
                if(isHighest):
                    result = Constants.SUCCESS_UPDATE_MAX
                else:
                    result = Constants.SUCCESS

        else: #NON-LEAF NODE
            #Find a place to go down the tree
            goodPlace = curNode.length
            for i in range(0, curNode.length):
                if(curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                    goodPlace = i
                    break
            result = self.insertBucketRec2(curNode.pointer[goodPlace], _interval, _bucketID, selEntry, newNode, recRec, recMax)
            if (result == Constants.SUCCESS_UPDATE_MAX):
                # Update max value for the node at the current possition
                if(goodPlace < curNode.length):
                    if(curNode.interval[goodPlace][Constants.HIGH] < _interval[Constants.HIGH]):
                        curNode.max[goodPlace] = _interval[Constants.HIGH]
                else:
                    curNode.max[goodPlace] = _interval[Constants.HIGH]
                # Check if the current max value is the highest value
                result = Constants.SUCCESS
                for i in range(0, curNode.length + 1):
                    if(_interval[Constants.HIGH] >= curNode.max[i]):
                        result = Constants.SUCCESS_UPDATE_MAX #Propagate to higher level
                        break
            elif(result == Constants.OVERFLOW):
                #Split node in the non-leaf level
                if(curNode.length >= Constants.MAX_NUM_ENTRY):
                    #No more place, split the current node and return OVERFLOW
                    tempEntry = IBEntry()
                    tempNode = IBNode()
                    #1. Move to buffer
                    buffer = IBNodeBuffer()
                    copyNodeToBuffer(buffer, curNode, recRec.selEntry.bucketID, recRec.selEntry.interval, recMax.curMax, recMax.newMax, recRec.newNode, goodPlace, Constants.MAX_NUM_ENTRY)
                    #2. Copy from buffer
                    copyNodeFromBuffer(buffer, curNode, 0, Constants.MAX_NUM_ENTRY / 2 - 1)
                    copyEntryFromBuffer(buffer, tempEntry, Constants.MAX_NUM_ENTRY / 2)
                    copyNodeFromBuffer(buffer, tempNode, Constants.MAX_NUM_ENTRY / 2 + 1, Constants.MAX_NUM_ENTRY)
                    #3. finalize
                    tempNode.isLeaf = curNode.isLeaf
                    tempNode.level = curNode.level
                    tempNode.length = Constants.MAX_NUM_ENTRY / 2
                    curNode.length = Constants.MAX_NUM_ENTRY / 2
                    for i in range(0, Constants.MAX_NUM_ENTRY / 2 + 1):
                        if (recMax.newMax < tempNode.max[i]):
                            recMax.newMax = tempNode.max[i]
                    if (recMax.newMax < tempEntry.interval[Constants.HIGH]):
                        recMax.newMax = tempEntry.interval[Constants.HIGH]
                    for i in range(0, Constants.MAX_NUM_ENTRY / 2 + 1):
                        if (recMax.curMax < curNode.max[i]):
                            recMax.curMax = curNode.max[i]
                    recRec.selEntry = tempEntry
                    recRec.newNode = tempNode
                    result = Constants.OVERFLOW

                else: #curNode.length < Constants.MAX_NUM_ENTRY
                    #1. Move to buffer
                    buffer = IBNodeBuffer()
                    copyNodeToBuffer(buffer, curNode, recRec.selEntry.bucketID, recRec.selEntry.interval, recMax.curMax, recMax.newMax, recRec.newNode, goodPlace, curNode.length)
                    #2. Copy from buffer
                    copyNodeFromBuffer(buffer, curNode, 0, curNode.length)
                    #3. finalize
                    curNode.length += 1
                    #return SUCCESS or SUCCESS_UPDATE_MAX
                    result = Constants.SUCCESS
                    for i in range(0, curNode.length + 1):
                        if (_interval[Constants.HIGH] >= curNode.max[i]):
                            result = Constants.SUCCESS_UPDATE_MAX
                            break

            elif(result == Constants.SUCCESS): #Do nothing
                result = Constants.SUCCESS
        return result

    ### Index the data into IB-Tree ###
    def insertBucket(self, _interval, _bucketID):
        selEntry = IBEntry()
        newNode = IBNode()
        recMax = recordMax()
        recRec = recordRec()

        result = Constants.ERROR
        if(self.rootNode is None):
            return result
        self.evaluation.setDBInterval(_interval)
        self.evaluation.countTotalBuckets()
        result = self.insertBucketRec(self.rootNode, _interval, _bucketID, selEntry, newNode, recRec, recMax)
        if (result == Constants.OVERFLOW):
            #Overflow in root -> increase the height of the tree
            newRoot = IBNode()
            newRoot.length = 1
            newRoot.level = self.rootNode.level + 1
            newRoot.isLeaf = False
            assign(newRoot.interval[0], recRec.selEntry.interval)
            newRoot.bucketID[0] = recRec.selEntry.bucketID
            if(newRoot.interval[0][Constants.HIGH] < recMax.curMax):
                newRoot.max[0] = recMax.curMax
            else:
                newRoot.max[0] = newRoot.interval[0][Constants.HIGH]
            newRoot.max[1] = recMax.newMax
            newRoot.pointer[0] = self.rootNode
            newRoot.pointer[1] = recRec.newNode
            self.rootNode = newRoot
            result = Constants.SUCCESS
        return result

    def insertBucketRec(self, curNode, _interval, _bucketID, selEntry, newNode, recRec, recMax):
        result = Constants.ERROR
        if(curNode is None):
            return result
        if(curNode.isLeaf == True): #LEAF-NODE
            #Find a place to insert
            if (curNode.length >= Constants.MAX_NUM_ENTRY):
                #No more place, split at Leaf-level and return overflow
                recRec.selEntry = IBEntry()
                recRec.newNode = IBNode()
                recMax.newMax = Constants.MIN_DISTANCE
                recMax.curMax = Constants.MIN_DISTANCE
                #Find a place to split
                goodPlace = curNode.length
                for i in range(0, curNode.length):
                    if(curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                        goodPlace = i
                        break
                #1. Move data to buffer
                buffer = IBNodeBuffer()
                copyNodeToBuffer(buffer, curNode, _bucketID, _interval, -1.0, -1.0, None, goodPlace, Constants.MAX_NUM_ENTRY)
                #2. Copy from buffer
                copyNodeFromBuffer(buffer, curNode, 0, Constants.MAX_NUM_ENTRY / 2 - 1)
                copyEntryFromBuffer(buffer, recRec.selEntry, Constants.MAX_NUM_ENTRY / 2)
                copyNodeFromBuffer(buffer, recRec.newNode, Constants.MAX_NUM_ENTRY / 2 + 1, Constants.MAX_NUM_ENTRY)
                #3. Finalize
                recRec.newNode.isLeaf = True
                recRec.newNode.level = 0
                recRec.newNode.length = Constants.MAX_NUM_ENTRY/2
                for i in range(0, Constants.MAX_NUM_ENTRY/2):
                    if (recMax.newMax < recRec.newNode.max[i]):
                        recMax.newMax = recRec.newNode.max[i]
                curNode.length = Constants.MAX_NUM_ENTRY/2
                for i in range(0, Constants.MAX_NUM_ENTRY/2):
                    if (recMax.curMax < curNode.max[i]):
                        recMax.curMax = curNode.max[i]
                result = Constants.OVERFLOW #No more place return overflow

            else: ##curNode.length < Constants.MAX_NUM_ENTRY##
                goodPlace = curNode.length
                #Check if the current max value is the highest value
                isHighest = True
                for i in range(0, curNode.length + 1):
                    if(_interval[Constants.HIGH] < curNode.max[i]):
                        isHighest = False
                        break
                for i in range(0, curNode.length):
                    if (curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                        goodPlace = i
                        break
                #1. Move to buffer
                buffer = IBNodeBuffer()
                copyNodeToBuffer(buffer, curNode, _bucketID, _interval, -1.0, -1.0, None, goodPlace, Constants.MAX_NUM_ENTRY)
                #2. Copy from buffer
                copyNodeFromBuffer(buffer, curNode, 0, curNode.length)
                #3. Finalize
                curNode.length += 1
                if(isHighest):
                    result = Constants.SUCCESS_UPDATE_MAX
                else:
                    result = Constants.SUCCESS

        else: #NON-LEAF NODE
            #Find a place to go down the tree
            goodPlace = curNode.length
            for i in range(0, curNode.length):
                if(curNode.interval[i][Constants.LOW] > _interval[Constants.LOW]):
                    goodPlace = i
                    break
            result = self.insertBucketRec(curNode.pointer[goodPlace], _interval, _bucketID, selEntry, newNode, recRec, recMax)
            if (result == Constants.SUCCESS_UPDATE_MAX):
                # Update max value for the node at the current possition
                if(goodPlace < curNode.length):
                    if(curNode.interval[goodPlace][Constants.HIGH] < _interval[Constants.HIGH]):
                        curNode.max[goodPlace] = _interval[Constants.HIGH]
                else:
                    curNode.max[goodPlace] = _interval[Constants.HIGH]
                # Check if the current max value is the highest value
                result = Constants.SUCCESS
                for i in range(0, curNode.length + 1):
                    if(_interval[Constants.HIGH] >= curNode.max[i]):
                        result = Constants.SUCCESS_UPDATE_MAX #Propagate to higher level
                        break
            elif(result == Constants.OVERFLOW):
                #Split node in the non-leaf level
                if(curNode.length >= Constants.MAX_NUM_ENTRY):
                    #No more place, split the current node and return OVERFLOW
                    tempEntry = IBEntry()
                    tempNode = IBNode()
                    #1. Move to buffer
                    buffer = IBNodeBuffer()
                    copyNodeToBuffer(buffer, curNode, recRec.selEntry.bucketID, recRec.selEntry.interval, recMax.curMax, recMax.newMax, recRec.newNode, goodPlace, Constants.MAX_NUM_ENTRY)
                    #2. Copy from buffer
                    copyNodeFromBuffer(buffer, curNode, 0, Constants.MAX_NUM_ENTRY / 2 - 1)
                    copyEntryFromBuffer(buffer, tempEntry, Constants.MAX_NUM_ENTRY / 2)
                    copyNodeFromBuffer(buffer, tempNode, Constants.MAX_NUM_ENTRY / 2 + 1, Constants.MAX_NUM_ENTRY)
                    #3. finalize
                    tempNode.isLeaf = curNode.isLeaf
                    tempNode.level = curNode.level
                    tempNode.length = Constants.MAX_NUM_ENTRY / 2
                    curNode.length = Constants.MAX_NUM_ENTRY / 2
                    for i in range(0, Constants.MAX_NUM_ENTRY / 2 + 1):
                        if (recMax.newMax < tempNode.max[i]):
                            recMax.newMax = tempNode.max[i]
                    if (recMax.newMax < tempEntry.interval[Constants.HIGH]):
                        recMax.newMax = tempEntry.interval[Constants.HIGH]
                    for i in range(0, Constants.MAX_NUM_ENTRY / 2 + 1):
                        if (recMax.curMax < curNode.max[i]):
                            recMax.curMax = curNode.max[i]
                    recRec.selEntry = tempEntry
                    recRec.newNode = tempNode
                    result = Constants.OVERFLOW

                else: #curNode.length < Constants.MAX_NUM_ENTRY
                    #1. Move to buffer
                    buffer = IBNodeBuffer()
                    copyNodeToBuffer(buffer, curNode, recRec.selEntry.bucketID, recRec.selEntry.interval, recMax.curMax, recMax.newMax, recRec.newNode, goodPlace, curNode.length)
                    #2. Copy from buffer
                    copyNodeFromBuffer(buffer, curNode, 0, curNode.length)
                    #3. finalize
                    curNode.length += 1
                    #return SUCCESS or SUCCESS_UPDATE_MAX
                    result = Constants.SUCCESS
                    for i in range(0, curNode.length + 1):
                        if (_interval[Constants.HIGH] >= curNode.max[i]):
                            result = Constants.SUCCESS_UPDATE_MAX
                            break

            elif(result == Constants.SUCCESS): #Do nothing
                result = Constants.SUCCESS
        return result


    ##############<Search>##############
    def search(self, _output, _interval):
        result = Constants.ERROR
        if (self.rootNode is None):
            return result
        #Database file
        #fin = open(self.ibTreeDataBase, "r")
        fin = open(self.ibTreeDataBase, "rb")
        result = self.searchRec(self.rootNode, _output, _interval, fin)
        fin.close()
        self.evaluation.setInterval(_interval)
        return result

    def searchRec(self, curNode, _output, _interval, fin):
        #Return all bucketIDs whose intervals intersect with a given interval
        result = Constants.SUCCESS
        if(curNode is None):
            print "+++++++++++++++++++++++++++<ERROR CODE: 3>++++++++++++++++++++++++++++++++"
            result = Constants.ERROR
            return result
        if(curNode.isLeaf):
            for i in range(0, curNode.length):
                self.evaluation.countSE()
                if(_interval[Constants.HIGH] < curNode.interval[i][Constants.LOW]):
                    break
                if(intersect(_interval, curNode.interval[i])):
                    _output.results.append(curNode.bucketID[i])
                    #Read DB
                    fin.seek(curNode.bucketID[i])
                    #for j in range(0, Constants.NUM_ROW_PER_BUCKET):
                    #    line = fin.readline()
                    #line = fin.readline()
                    #bytes = fin.read(256000)
                    bytes = fin.read(Constants.BLOCK_SIZE)
                    #bytes = fin.read(Constants.BLOCK_SIZE_3)
                    #bytes = fin.read(Constants.BLOCK_SIZE_2)
                    #fin.seek(2000000, 1)
                    #bytes = fin.read(Constants.BLOCK_SIZE_2)
                    #fin.seek(2000000, 1)
                    #bytes = fin.read(Constants.BLOCK_SIZE_2)
                    #fin.seek(2000000, 1)
                    #bytes = fin.read(Constants.BLOCK_SIZE_2)
                    #fin.seek(curNode.bucketID[i])
                    #line = list(islice(fin, Constants.NUM_ROW_PER_BUCKET))
                    self.evaluation.countReturnBuckets()
        else:
            index = 0
            #All entries and children (except last child)
            for i in range(0, curNode.length):
                if (result == Constants.ERROR):
                    return result
                self.evaluation.countSE()
                index += 1
                if((_interval[Constants.LOW] <= curNode.max[i]) & (curNode.pointer[i] is not None)):
                    result = self.searchRec(curNode.pointer[i], _output, _interval, fin)
                if (_interval[Constants.HIGH] < curNode.interval[i][Constants.LOW]):
                    return result
                if(intersect(_interval, curNode.interval[i])):
                    _output.results.append(curNode.bucketID[i])
                    #Read DB
                    fin.seek(curNode.bucketID[i])
                    #for j in range(0, Constants.NUM_ROW_PER_BUCKET):
                    #    line = fin.readline()
                    line = fin.readline()
                    #fin.seek(curNode.bucketID[i])
                    #line = list(islice(fin, Constants.NUM_ROW_PER_BUCKET))
                    self.evaluation.countReturnBuckets()
            #last child
            if ((_interval[Constants.LOW] <= curNode.max[index]) & (curNode.pointer[index] is not None)):
                result = self.searchRec(curNode.pointer[index], _output, _interval, fin)
        return result

    def searchImprints(self, _output, _interval):
        result = Constants.ERROR
        if (self.rootNode is None):
            return result
        result = self.searchRecImprints(self.rootNode, _output, _interval)
        self.evaluation.setInterval(_interval)
        return result

    def searchRecImprints(self, curNode, _output, _interval):
        #Return all bucketIDs whose intervals intersect with a given interval
        result = Constants.SUCCESS
        if(curNode is None):
            print "+++++++++++++++++++++++++++<ERROR CODE: 3>++++++++++++++++++++++++++++++++"
            result = Constants.ERROR
            return result
        if(curNode.isLeaf):
            for i in range(0, curNode.length):
                self.evaluation.countSE()
                if(compareImprints(_interval[Constants.HIGH], curNode.interval[i][Constants.LOW]) < 0):
                    break
                if(intersectImprints(_interval, curNode.interval[i])):
                    _output.results.append(curNode.bucketID[i])
                    self.evaluation.countReturnBuckets()
        else:
            index = 0
            #All entries and children (except last child)
            for i in range(0, curNode.length):
                if (result == Constants.ERROR):
                    return result
                self.evaluation.countSE()
                index += 1
                if((compareImprints(_interval[Constants.LOW], curNode.max[i]) <=0) & (curNode.pointer[i] is not None)):
                    result = self.searchRecImprints(curNode.pointer[i], _output, _interval)
                if (compareImprints(_interval[Constants.HIGH], curNode.interval[i][Constants.LOW]) < 0):
                    return result
                if(intersectImprints(_interval, curNode.interval[i])):
                    _output.results.append(curNode.bucketID[i])
                    self.evaluation.countReturnBuckets()
            #last child
            if ((compareImprints(_interval[Constants.LOW], curNode.max[index]) <= 0) & (curNode.pointer[index] is not None)):
                result = self.searchRecImprints(curNode.pointer[index], _output, _interval)
        return result

    ##############<Print>##############
    numPrint = 0 #Note: this variable is only used for testing, it should be commented
    def printIBTree(self):
        #print IB-Tree
        result = Constants.ERROR
        if(self.rootNode is None):
            return result
        self.numPrint = 0
        result = self.printIBTreeRec(self.rootNode, "", 0)
        print "=================================== Number of elements: " + str(self.numPrint) + " ==========================================="
        return result

    def printIBTreeRec(self, curNode, _strLable, _level):
        #print IB-Tree recursively
        result = Constants.ERROR
        if(curNode is None):
            print "++++++++++++++++++++++++++++<ERROR CODE: 0>++++++++++++++++++++++++++++++++"
            return result
        strLable = ""
        if(_strLable != ""):
            strLable = _strLable + "." + str(_level)
        else:
            strLable = str(_level)
        if(curNode.isLeaf):
            print "Leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t" + str(curNode.bucketID[i]) + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t #: " + str(i)
                self.numPrint += 1
        else:
            print "Non-leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t" + str(curNode.bucketID[i]) + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t Max: " + str(curNode.max[i])
                self.numPrint += 1
                result = self.printIBTreeRec(curNode.pointer[i], strLable, i)
                if(result == Constants.ERROR):
                    print "++++++++++++++++++++++++++++<ERROR CODE: 1>++++++++++++++++++++++++++++++++"
                    return result
            #print last child
            print strLable + "\t" + "Last child" + "\t [" + "Low" + ", " + "High" + "]" + "\t Max: " + str(curNode.max[curNode.length])
            result = self.printIBTreeRec(curNode.pointer[curNode.length], strLable, curNode.length)
            if(result == Constants.ERROR):
                print "++++++++++++++++++++++++++++<ERROR CODE: 2>++++++++++++++++++++++++++++++++"
                return result
        result = Constants.SUCCESS
        return result

    ##############<TODO: Write metadata>##############
    def writeMetaData(self):
        self.nextPositionIndex = 0
        self.writeMetaDataRec(self.rootNode, -1)
        return

    def writeMetaDataRec(self, curNode, parentID):
        #Format:
        #NodexIndex    Level   Length  Parent  isLeaf(False)  Interval:[min max];...[min max];    Max: max;...max; BucketID: ID;ID;"\n"

        #1. Write current node data
        curIndex = self.nextPositionIndex
        strWrite = str(curIndex) + "\t" + str(curNode.level) + "\t" + str(curNode.length) + "\t"
        strWrite += str(parentID) + "\t" + str(curNode.isLeaf) + "\t"
        #1.1 interval information
        strWrite += "Interval: "
        for i in range(0, curNode.length):
            strWrite += str(curNode.interval[i][Constants.LOW]) + " " + str(curNode.interval[i][Constants.HIGH]) + "; "
        #1.2 max information & BucketID
        strWrite += "\t" + "Max: "
        for i in range(0, curNode.length):
            strWrite += str(curNode.max[i]) + "; "
        #last max
        strWrite += str(curNode.max[curNode.length]) + "; "
        #BucketID
        strWrite += "\t" + "BucketID: "
        for i in range(0, curNode.length):
            strWrite += str(curNode.bucketID[i]) + "; "
        strWrite += "\n"

        #1.3 write to file
        fout = open(self.ibTreeMetaData, "a+")
        fout.write(strWrite)
        fout.close()
        #1.4 increase next position index
        self.nextPositionIndex += 1

        #2. Recursive function call
        if(curNode.isLeaf == False):
            for i in range(0, curNode.length):
                self.writeMetaDataRec(curNode.pointer[i], curIndex)
            #last child
            self.writeMetaDataRec(curNode.pointer[curNode.length], curIndex)
        return

    def readMetaData(self):
        #Read and rebuild IB-Tree
        result = Constants.ERROR

        #commented for batch testing
        #if(self.rootNode is not None):
        #    str = raw_input("IB-Tree is not Null! All data in the IB-Tree will be lost! Do you want to continue (y/n)?:")
        #    if(str != "y"):
        #        return result

        #self.rootNode = newIBPlusNode(False)
        #pdb.set_trace()
        self.pointerStack.clear()
        result = self.readMetaDataRec()
        rootP = self.pointerStack.pop()
        #pdb.set_trace()
        while(rootP.index != 0):
            rootP = self.pointerStack.pop()
        self.rootNode = rootP.pointer
        return result

    def readMetaDataRec(self):
        result = Constants.ERROR
        fin = open(self.ibTreeMetaData, "r")
        line = fin.readline()
        while (line != ""):
            values = line.split("\t")
            #pdb.set_trace()
            if(values[0] != '-1'):
                #nodeIndex = values[0]  #level = values[1]  #length = values[2] #parentID = values[3]
                #isLeaf = values[4] #interval = values[5]   #Max/Count = values[6]
                node = IBNode() #create a new node
                node.isLeaf = (values[4] == 'True')
                node.level = int(values[1])
                #node.length = int(values[2])
                node.length = 0
                length1 = int(values[2])
                parentID = int(values[3])
                listIntervals = values[5].split(':')[1].strip().split(";")
                for i in range(0, length1):
                    data = listIntervals[i].strip().split(' ')
                    node.interval[i][Constants.LOW] = float(data[0])
                    node.interval[i][Constants.HIGH] = float(data[1])

                listMaxValues = values[6].split(':')[1].strip().split(";")
                for i in range(0, length1 + 1):
                    node.max[i] = float(listMaxValues[i])

                listBucketID = values[7].split(':')[1].strip().split(";")
                for i in range(0, length1):
                    node.bucketID[i] = int(listBucketID[i])

                if(node.isLeaf): #Leaf-node
                    node.length = int(values[2])

                nodeP = nodePointer()
                nodeP.pointer = node
                nodeP.index = int(values[0])
                #pdb.set_trace()
                if(parentID == -1):
                    self.pointerStack.push(nodeP)
                else:
                    curP = self.pointerStack.pop()
                    while(curP.index != parentID):
                        curP = self.pointerStack.pop()

                    curP.pointer.pointer[curP.pointer.length] = node
                    curP.pointer.length += 1
                    self.pointerStack.push(curP)
                    self.pointerStack.push(nodeP)

            line = fin.readline()
        fin.close()
        return result
    ##############<Get root node: Only used for copystructure function>##############
    def getRootNode(self):
        return self.rootNode

    def getSE(self):
        return self.evaluation.getSE()

    def printEvalInfo(self):
        self.evaluation.printEvalInfo()
        return Constants.SUCCESS
    ##############<Update: actually, we don't really need this function>##############
    def updateBucket(self):
        return False

    ##############<Delete: we don't really need this function>##############
    def deleteBucket(self):
        return False

    ##############<Rotate>##############
    def rotate(self):
        return False