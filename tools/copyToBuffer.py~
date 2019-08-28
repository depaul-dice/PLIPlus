#!/usr/bin/python

from PLITypes import Constants
from tools.assignInterval import assign

def copyBucketToBuffer(buffer, node, value, position, length):
    for i in range(0, position):
        buffer.bucketID[i] = node.bucketID[i]
    buffer.bucketID[position] = value
    for i in range(position + 1, length + 1):
        buffer.bucketID[i] = node.bucketID[i - 1]
    result = True
    return result

def copyIntervalToBuffer(buffer, node, value, position, length):
    for i in range(0, position):
        assign(buffer.interval[i], node.interval[i])
    assign(buffer.interval[position], value)
    for i in range(position + 1, length + 1):
        assign(buffer.interval[i], node.interval[i - 1])
    result = True
    return result

def copyMaxToBuffer(buffer, node, curMax, newMax, position, length): #non-leaf node
    for i in range(0, position + 1):
        buffer.max[i] = node.max[i]
    if(buffer.interval[position][Constants.HIGH] < curMax):
        buffer.max[position] = curMax
    else:
        buffer.max[position] = buffer.interval[position][Constants.HIGH]
    buffer.max[position + 1] = newMax
    for i in range(position + 2, length + 2):
        buffer.max[i] = node.max[i - 1]
    result = True
    return result

def copyPointerToBuffer(buffer, node, value, position, length):
    for i in range(0, position + 1):
        buffer.pointer[i] = node.pointer[i]
    buffer.pointer[position + 1] = value
    for i in range(position + 2, length + 2):
        buffer.pointer[i] = node.pointer[i - 1]
    result = True
    return result

def copyNodeToBuffer(buffer, node, bucket, interval, curMax, newMax, pointer, position, length):
    copyBucketToBuffer(buffer, node, bucket, position, length)
    copyIntervalToBuffer(buffer, node, interval, position, length)
    if((curMax > 0.0) & (newMax > 0.0)):
        copyMaxToBuffer(buffer, node, curMax, newMax, position, length)
    else: #Leaf-node
        for i in range(0, length + 1):
            buffer.max[i] = buffer.interval[i][Constants.HIGH]
    if (pointer is not None):
        copyPointerToBuffer(buffer, node, pointer, position, length)
    result = True
    return result