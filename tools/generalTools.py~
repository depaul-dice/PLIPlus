#!/usr/bin/python

from PLITypes import Constants
from tools import operations
import math

def copyRowData(d_row, s_row):
    for i in range(0, len(s_row)):
        d_row.append(s_row[i])
    return Constants.FUNC_TRUE

def findGoodPlace(node, key, isLeaf):
    if(node is None):
        return -1
    if(isLeaf):
        goodPlace = node.length - 1
    else:
        goodPlace = node.length
    if(Constants.IMPRINTS_MODE):
        for i in range(0, node.length):
            if(operations.compareImprints(key, node.interval[i][Constants.LOW]) <= 0):
                goodPlace = i
                break
    else:
        for i in range(0, node.length):
            if(key <= node.interval[i][Constants.LOW]):
                goodPlace = i
                break
    #print goodPlace
    return goodPlace

def imprintsHash(arrayV):
    result = 0
    for i in range(0, len(arrayV)):
        temp = hashValue(arrayV[i])
    result |= temp
    return result

def hashValue(value):
    result = int(0)
    if(Constants.IMPRINTS_MODE_BD):
        if(value >= 203): #[203, 403) -- 2 bits
            if(value > 303):
                result |= 0x80000000
            else:
                result |= 0x40000000
        elif(value >= 103): #[103, 203) -- 10 bits
            shift = int(math.ceil(float(value - 103)/10))
            temp = 0x00100000
            temp = temp << shift
            result |= temp
        else: #[3, 103) -- 20 bits
            shift = int(math.ceil(float(value - 3)/5))
            #print "Shift (bits): " + str(shift)
            temp = 0x00000001
            temp = temp << shift
            result |= temp
    else:
        if(value >= 20): #[20, 40)
            result |= 0x80000000
        elif(value >= 10): #[10, 20)
            if(value > 15):
                result |= 0x40000000
            else:
                result |= 0x20000000
        elif(value >= 5):#[5, 10)
            shift = int(math.ceil(float(value - 5)/5))
            temp = 0x01000000
            temp = temp << shift
            result |= temp
        else: #[0, 5)
            shift = int(math.ceil(float(value - 0)/24))
            temp = 0x00000001
            temp = temp << shift
            result |= temp
    return result

def zonemapsHash(arrayV):
    result = [0.0 for x in range(0, 2)]
    result[0] = arrayV[0]
    result[1] = arrayV[0]
    for i in range(0, len(arrayV)):
        if(result[0] > arrayV[i]):
            result[0] = arrayV[i]
        if(result[1] < arrayV[i]):
            result[1] = arrayV[i]
    return result
