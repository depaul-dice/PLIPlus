#!/usr/bin/python

from PLITypes import Constants
def intersect(interval1, interval2):
    if(Constants.IMPRINTS_MODE):
        result = intersectImprints(interval1, interval2)
    else:
        result = intersect_number(interval1, interval2)
    return result

def intersect_number(interval1, interval2):
    if(interval1[0] > interval2[0]):
        maxV = interval1[0]
    else:
        maxV = interval2[0]
    if (interval1[1] > interval2[1]):
        minV = interval2[1]
    else:
        minV = interval1[1]
    result = (maxV <= minV)
    return result

def inside(interval, value):
    return ((interval[Constants.LOW] <= value) & (interval[Constants.HIGH] >= value))

def insideImprints(Interval, key):
    res1 = (compareImprints(Interval[Constants.LOW], key) <= 0)
    res2 = (compareImprints(Interval[Constants.HIGH], key) >= 0)
    res = res1 & res2
    return res

def intersect_v2(interval1, interval2):
    #result = (interval1[0] >= interval2[0] & interval1[0] <= interval2[1]) | (interval1[1] >= interval2[0] & interval1[1] <= interval2[1]) | (interval2[0] >= interval1[0] & interval2[0] <= interval1[1]) | (interval2[1] >= interval1[0] & interval2[1] <= interval1[1])
    result = (interval1[0] >= interval2[0] & interval1[0] <= interval2[1]) | (
    interval1[1] >= interval2[0] & interval1[1] <= interval2[1]) | (
             interval2[0] >= interval1[0] & interval2[0] <= interval1[1]) | (
             interval2[1] >= interval1[0] & interval2[1] <= interval1[1])
    return result

#Return 0: imprints1 == imprints2
#       1: imprints1 > imprints2
#       -1: imprints1 < imprints2
def compareImprints(imprints1, imprints2):
    # 0. Calculate number of bits 1
    bin1 = bin(int(imprints1)).count('1')
    bin2 = bin(int(imprints2)).count('1')
    if(int(imprints1) < 0):
        bin1 += 1
    if(int(imprints2) < 0):
        bin2 += 1

    # 1. Compare the number of bits 1
    if(bin1 > bin2):
        return 1
    if(bin1 < bin2):
        return -1

    # 2. Compare the values (consider as unsigned number)
    if(bin1 == bin2):
        if(bin1 > 0):
            if(int(imprints1) > int(imprints2)):
                return 1
            if(int(imprints1) < int(imprints2)):
                return -1
            return 0
        if(bin1 < 0):
            if(int(imprints1) > int(imprints2)):
                return -1
            if(int(imprints1) < int(imprints2)):
                return 1
            return 0
        return 0

def intersectImprints(Interval1, Interval2):
    res0 = compareImprints(Interval1[0], Interval2[0])
    res1 = compareImprints(Interval1[1], Interval2[1])
    maxV = 0
    minV = 0
    if(res0 > 0):
        maxV = Interval1[0]
    else:
        maxV = Interval2[0]

    if(res1 > 0):
        minV = Interval2[1]
    else:
        minV = Interval1[1]

    res = compareImprints(minV, maxV)

    return res

def intersectImprints_v2(Interval1, Interval2):

    res0 = int(Interval1[0]) | int(Interval1[1])
    res1 = int(Interval2[0]) | int(Interval2[1])
    res = ovelapImprints(res0, res1)
    return res

def insideImprints_v2(Interval, key):
    res0 = int(Interval[0]) | int(Interval[1])
    res = ovelapImprints(res0, key)
    return res

def ovelapImprints(imprints1, imprints2):
    result = 0
    result = int(imprints1) & int(imprints2)
    if(result > 0):
        result = 1
    if (result < 0):
        result = 1
    return result
