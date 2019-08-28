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
from PLITypes.PLIDataType import *
from tools.operations import *
from tools.assignInterval import assign
from tools.copyFromBuffer import *
from tools.copyToBuffer import *
import pdb
class PLI():
    def __init__(self):
        self.clustered = clusteredData()
        self.countC = 0
        self.indexC = 0
        self.overflow = overflowData()
        self.countO = 0
        self.database = "PLIdata.dat"
        self.offset = 0
        self.metadata = "PLImeta.dat"

    def setFile(self, databaseFile, metadataFile):
        self.database = databaseFile
        self.metadata = metadataFile
        return
    ##############<Insert>##############
    def insertTuple(self, key, data):
        result = Constants.ERROR
        if(self.countC < Constants.MAX_SIZE_CLUSTERED):
            fout = open(self.database, "a")
            #1. Append key value to PLI
            self.indexC += 1
            if(self.clustered.interval[self.countC][Constants.LOW] > key):
                self.clustered.interval[self.countC][Constants.LOW] = key
            if(self.clustered.interval[self.countC][Constants.HIGH] < key):
                self.clustered.interval[self.countC][Constants.HIGH] = key
            if(self.indexC == Constants.NUM_ROW_PER_BUCKET):
                self.offset = fout.tell()
                self.countC += 1
                self.indexC = 0
                self.clustered.bucketID[self.countC] = self.offset
            #2. Write data to database
            fout.write(str(key) + "\t" + data + "\n")
            fout.close()
            result = Constants.SUCCESS
        else:
            result = self.insertTupleOverflow(key, data)
        return result

    def insertTupleOverflow(self, key, data):
        #1. Append key to overflow page
        fout = open(self.database, "a")
        if(self.countO == 0):
            self.offset = fout.tell()
            self.overflow.pointer = self.offset
        self.countO += 1
        if(self.overflow.min > key):
            self.overflow.min = key
        if(self.overflow.max < key):
            self.overflow.max = key
        #2. Write to database
        fout.write(str(key) + "\t" + data + "\n")
        fout.close()
        return Constants.OVERFLOW

    ##############<Update: not really necessary>##############
    def insertBucket(self, _interval, _bucketID):
        result = Constants.ERROR
        return result

    def insertBucketRec(self, curNode, _interval, _bucketID, selEntry, newNode, recRec, recMax):
        result = Constants.ERROR
        return result

    ##############<Search>##############
    def searchBucketIDs(self, _output, _interval):
        result = Constants.SUCCESS
        if(self.countO > 0):
            _output.append(self.overflow.pointer)
        for i in range(0, self.countC):
            if(intersect(_interval, self.clustered.interval[i])):
                _output.append(self.clustered.bucketID[i])
        return result

    def searchTuples(self, _output, _interval):
        #Return all bucketIDs whose intervals intersect with a given interval
        result = Constants.SUCCESS
        fin = open(self.database, "r")
        #Scan in clustered part
        for i in range(0, self.countC):
            if(intersect(_interval, self.clustered.interval[i])):
                fin.seek(self.clustered.bucketID[i], 0)
                numEntry = Constants.NUM_ROW_PER_BUCKET
                if(i == self.countC - 1):
                    numEntry = self.indexC
                for j in range(0, numEntry):
                    line = fin.readline()
                    if(line == ''):
                        break;
                    values = line.rstrip('\n').split('\t', 2)
                    key = float(values[0])
                    if(inside(_interval, key)):
                        _output.append(values[1])
        #Scan in the overflow page
        if(self.countO > 0):
            fin.seek(self.overflow.pointer, 0)
            for line in fin:
                values = line.rstrip('\n').split('\t', 2)
                if(len(values) >= 2):
                    key = float(values[0])
                    if(inside(_interval, key)):
                        append = 1
                        #_output.append(values[1])
        fin.close()
        return result

    ##############<Print>##############
    def printPLI(self):
        result = Constants.ERROR
        return result

    def printPLIRec(self, curNode, _strLable, _level):
        #print PLI recursively
        result = Constants.ERROR
        return result
    def writeMetaData(self):
        fout = open(self.metadata, "w")
        ###<format>###
        # Cluster: (number of buckets in clustered part)
        # Bucket:   n  size    Interval    Pointer
        # Overflow: (number of tuples in overflow page)   pointer
        if(self.countC > 0):
            fout.write("Cluster:\t" + str(self.countC) + "\n")
            for i in range(0, self.countC):
                fout.write("Bucket:\t" + str(i) + "\t")
                if(i < self.countC - 1):
                    fout.write("1000" + "\t")
                else:
                    fout.write(str(self.indexC) + "\t")
                fout.write(str(self.clustered.interval[i][0]) + "\t" + str(self.clustered.interval[i][1]) + "\t")
                fout.write(str(self.clustered.bucketID[i]) + "\n")
        if(self.countO > 0):
            fout.write("Overflow:\t" + str(self.countO) + "\t")
            fout.write(str(self.overflow.pointer) + "\n")
        fout.close()
        return
    def readMetaData(self):
        fin = open(self.metadata, "r")
        for line in fin:
            values = line.split('\t')
            if(values[0] == 'Cluster:'):
                self.countC = int(values[1])
            elif(values[0] == 'Bucket:'):
                bkIndex = int(values[1])
                self.clustered.interval[bkIndex][0] = float(values[2])
                self.clustered.interval[bkIndex][1] = float(values[3])
                self.clustered.bucketID[bkIndex] = float(values[4])
            elif(values[0] =='Overflow:'):
                self.countO = int(values[1])
                self.overflow.pointer = float(values[2])
                #Not necessary to use min & max, we always read overflow
        fin.close()
        return
    ##############<Update: actually, we don't really need this function>##############
    def updateBucket(self):
        return False

    ##############<Delete: we don't really need this function>##############
    def deleteBucket(self):
        return False
