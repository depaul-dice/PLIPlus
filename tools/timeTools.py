#!/usr/bin/python

import time

class timer():
    #starCounter = 0.0
    #endCounter = 0.0
    #result = 0.0 #
    #resultInSecond = 0.0
    #resultInMilliSecond = 0.0
    #resultInMicroSecond = 0.0
    def __init__(self):
        self.starCounter = 0.0
        self.endCounter = 0.0
        self.result = 0.0 #
        self.resultInSecond = 0.0
        self.resultInMilliSecond = 0.0
        self.resultInMicroSecond = 0.0
        return

    def start(self):
        self.starCounter = time.time()
        return

    def end(self):
        self.endCounter = time.time()
        self.result += self.endCounter - self.starCounter
        self.resultInSecond = self.result
        self.resultInMilliSecond = self.result * 1000.0
        self.resultInMicroSecond = self.result * 1000000.0
        return

    def reStart(self):
        self.starCounter = time.time()
        self.endCounter = 0.0
        self.result = 0.0
        self.resultInSecond = 0.0
        self.resultInMilliSecond = 0.0
        self.resultInMicroSecond = 0.0
        return

    def reStartValue(self, value):
        self.starCounter = time.time()
        self.endCounter = 0.0
        self.result = value
        self.resultInSecond = value
        self.resultInMilliSecond = value * 1000.0
        self.resultInMicroSecond = value * 1000000.0
        return

    def getResult(self):
        return self.result

    def getResultInSecond(self):
        return self.resultInSecond

    def getResultInMilliSecond(self):
        return self.resultInMilliSecond

    def getResultInMicroSecond(self):
        return self.resultInMicroSecond