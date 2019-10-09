#!/usr/bin/python

from PLITypes import Constants
from tools.assignInterval import assign

def copyBucketFromBuffer(buffer, node, start, end): #[start, end]
    index = 0
    for i in range(start, end + 1):
        node.bucketID[index] = buffer.bucketID[i]
        index += 1
    result = True
    return result

def copyInterval(des, des_Start, src, src_Start, length):
    for i in range (0, length):
        assign(des.interval[des_Start + i], src.interval[src_Start + i])
    return True

def copyLeafInterval_Dis(des, des_Start, src, src_Start, length):
    for i in range (0, length):
        assign(des.interval[des_Start + i], src.interval[src_Start + i])
        des.dis[des_Start + i] = src.dis[src_Start + i]
    return True

def copyIntervalFromBuffer(buffer, node, start, end): #[start, end]
    index = 0
    for i in range(start, end + 1):
        assign(node.interval[index], buffer.interval[i])
        index += 1
    result = True
    return result

def copyMaxFromBuffer(buffer, node, start, end):
    index = 0
    for i in range(start, end + 1):
        node.max[index] = buffer.max[i]
        #print i, range(start, end + 1)
        index += 1
    result = True
    return result

def copyPointerFromBuffer(buffer, node, start, end):
    index = 0
    for i in range(start, end + 1):
        node.pointer[index] = buffer.pointer[i]
        index += 1
    result = True
    return result

def copyEntryFromBuffer(buffer, entry, position):
    assign(entry.interval, buffer.interval[position])
    entry.max = buffer.max[position]
    entry.bucketID = buffer.bucketID[position]

def copyNodeFromBuffer(buffer, node, start, end):
    copyBucketFromBuffer(buffer, node, start, end)
    copyIntervalFromBuffer(buffer, node, start, end)
    copyMaxFromBuffer(buffer, node, start, end + 1)
    copyPointerFromBuffer(buffer, node, start, end + 1)
