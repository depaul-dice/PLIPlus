#!/usr/bin/python

from PLITypes import Constants
from PLITypes.dataTypes import *
from PLITypes.IBPlusNode import *
from PLITypes.dataTypes import *
from tools.assignInterval import *
from tools.copyFromBuffer import *
from tools.copyToBuffer import *
from tools.operations import *
from tools.generalTools import *
import sys
import pdb
from IBTree import IBTree

import math
#Require to install mmh and bitarray:
#   sudo pip install mmh3
#   sudo pip install bitarray
from bitarray import bitarray
import mmh3
#MAX_NUM_LOAD = 2400000
#MAX_ROW_DB = 295101150
#MAX_NUM_ROW_BUCKET_COLUMNAR = 12000
#MAX_BF_PAGE = 100
#UNCOMPRESSED = 0
#COMPRESSED = 1
#MAX_VALUE = 400000000 # used for BF hashing function
# n : number of entries = 100
# m : number of bits = 8 * 100 * 8 = 6400 bit
#

class BloomFilterGenerator():
    def __init__(self):
        self.n = 1000 # number of entries
        self.m = 8000 # number of bits = 8 * 1000
        self.k = self.find_opt_k() # Number of hash functions k = m/n * log(2)

    def __init__(self, _n, _m):
        self.n = _n
        self.m = _m
        self.k = self.find_opt_k()

    def find_opt_k(self):
        k = (self.m / self.n) * math.log(2)
        return int(k)

    def getBF(self, input):
        BF = biarray(self.m)
        BF.setall(0)
        len = len(input)
        for i in range(0, len):
            for j in range(0, self.k):
                pos = mmh3.hash(input[i],j) % self.k
                BF[pos] = True
        return BF

    def getHash(self, value):
        listPos = []
        for j in range(0, self.k):
            pos = mmh3.hash(value, j) % self.k
            listPos.append(pos)
        return listPos

    def check(self, BF, value):
        listPos = self.getHash(value)
        for j in range(0, len(listPos)):
            if(BF[listPos[j]] == True):
                continue
            else:
                return False
        return True
