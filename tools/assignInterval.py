#!/usr/bin/python

from PLITypes import Constants
def assign(destination, source):
    destination[Constants.LOW] = source[Constants.LOW]
    destination[Constants.HIGH] = source[Constants.HIGH]
    return Constants.FUNC_TRUE
