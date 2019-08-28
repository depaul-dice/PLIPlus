#!/usr/bin/python

#TODO:
# 1. Read data from PostgresQL (Tables: trips_proj5
#   (clustered on trip_distance), trips_proj6 (clustered on tip_amount),
#   trips_proj7 (clustered on total_amount))
# 2. Build the imprints extension on id columns of all above tables (10000 col/bucket)
# 3. Write the output to trips_proj5_imprints.dat, trips_proj6_imprints.dat and
#   trips_proj7_imprints.dat as well as the length information of these imprints
#   at length_Info5.dat, length_Info6.dat, length_Info7.dat

# 4. Do the test: Read these output files and do the joining between columns
# 5. Report the amount of data accessed.
# 6. ...

import psycopg2
from IBTree import IBTree
from PLITypes.IBEntry import *
from PLITypes.dataTypes import *
from tools.operations import *
from IBPlusTree import IBPlusTree
from tools.timeTools import  *
from tools.generalTools import  *
import re
from decimal import *
from random import uniform, randint
import os
import math
from os import listdir
from os.path import isfile, join

MAX_NUM_LOAD = 2000000
MAX_ROW_DB = 295101150
MAX_NUM_ROW_BUCKET_COLUMNAR = 10000
NUM_MILESTONE = 8
NUM_INTERVAL = NUM_MILESTONE / 2 - 1
UNCOMPRESSED = 0
COMPRESSED = 1

def binarySort(ListValue, start, end):
    if(start >= end):
        return
    if(start == (end - 1)):
        if(ListValue[end] < ListValue[start]):
            temp = ListValue[start]
            ListValue[start] = ListValue[end]
            ListValue[end] = temp
        return

    mid = int((end + start)/2)
    #Move mid to top
    temp = ListValue[mid]
    ListValue[mid] = ListValue[start]
    ListValue[start] = temp

    sort = start
    for i in range(start + 1, end + 1):
        if(ListValue[i] < ListValue[start]):
            sort += 1
            temp = ListValue[sort]
            ListValue[sort] = ListValue[i]
            ListValue[i] = temp
    binarySort(ListValue, start, sort)
    binarySort(ListValue, sort + 1, end)
    return

def buildImprints(listofID, count, output):
    if(count > 0 and listofID != None):
        # 1. Sort the list of ID
        binarySort(listofID, 0, count - 1)
        selIntervals = [0 for x in range(NUM_INTERVAL)]
        baseValues = [0 for x in range(NUM_INTERVAL)]
        base = listofID[0]
        delta = 0
        # 2. Find the biggest gaps
        for i in range(1, count):
            delta = listofID[i] - base
            for j in range(NUM_INTERVAL):
                if(delta > selIntervals[j]):
                    for t in range(j + 1, NUM_INTERVAL):
                        selIntervals[t] = selIntervals[t - 1]
                        baseValues[t] = baseValues[t - 1]
                    selIntervals[j] = delta
                    baseValues[j] = base
                    break
            base = listofID[i]
        # 2.1 Order the biggest gaps following the base values
        for i in range(NUM_INTERVAL):
            for j in range(i + 1, NUM_INTERVAL):
                if(baseValues[i] > baseValues[j]):
                    tempV = baseValues[i]
                    baseValues[i] = baseValues[j]
                    baseValues[j] = tempV
                    tempI = selIntervals[i]
                    selIntervals[i] = selIntervals[j]
                    selIntervals[j] = tempI
        # 3. Write the milestones
        output[0] = listofID[0]
        output[NUM_MILESTONE - 1] = listofID[count - 1]
        for i in range(NUM_INTERVAL):
            output[i * 2 + 1] = baseValues[i]
            output[(i + 1) * 2] = baseValues[i] + selIntervals[i]
    return

def writeImprints(imprints, outputFile, lengthFile, mode):
    if (outputFile != None and lengthFile != None):
        if(mode == UNCOMPRESSED): #uncompressed mode
            for i in range(NUM_MILESTONE):
                outputFile.write(str(imprints[i]) + "\n")
                lengthFile.write("8" + "\n")
        else: #compressed mode
            for i in range(NUM_MILESTONE):
                #TODO: Write in the compressed mode
                outputFile.write(imprints[i])
                #lengthFile.write("")
    else:
        print("Error: writeImprints function: Null output files!")
    return

def build_imprints_from_DB(outputImprints, lengthImprints, mode):
    # 1. Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 2. Get data from database to build imprints
    # Output files
    foutImprints = open(outputImprints, 'a')
    foutLength = open(lengthImprints, 'a')

    if(mode == 0):# trip_distance
        array = [0.5, 0.7, 0.85, 1.0, 1.15, 1.3, 1.45, 1.6, 1.75, 2.0, 2.3, 2.6, 2.8, 3.0, 3.5, 4.0, 5.0, 7.0, 10.0]
        tableName = 'trips5'
        colName = 'trip_distance'
    elif(mode == 1): # tip_amount
        array = [0.0, 0.3, 0.6, 0.8, 1.0, 1.2, 1.6, 1.8, 2.0, 2.35, 2.7, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
        tableName = 'trips6'
        colName = 'tip_amount'
    else: # total_amount
        array = [6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 15.0, 17.0, 19.0, 22.0, 25.0, 40.0]
        tableName = 'trips7'
        colName = 'total_amount'
    length = len(array)
    print "Total iterations: " + str(length + 1)
    numBucket = 0;
    count = 0;
    index = 0;
    listID = [0 for x in range(MAX_NUM_ROW_BUCKET_COLUMNAR)]
    output = [0 for x in range(NUM_MILESTONE)]
    loop = 0 #Only use for the mode 1
    i = 0
    while (i < length + 1):
        print "Round i: " + str(i)
        if(mode == 1): # Mode 1
            if(i == 0): # Loop for 6 times
                query = "select id from " + str(tableName) + " where " + str(colName) + " <= " + str(array[i]) + " offset " + str(loop * 20000000) + " rows fetch first 20000000 rows only;"
                print "\tLoop: " + str(loop)
                loop = loop + 1
                if(loop < 6):
                    i = i - 1 #Continue to loop until 6
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " > " + str(array[i - 1]) + " and " + str(colName) + " <= " + str(array[i]) + ";"
        else: # Mode 0 & 2
            if(i == 0):
                query = "select id from " + str(tableName) + " where " + str(colName) + " < " + str(array[i]) + ";"
            elif(i == length):
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1])+ ";"
            else:
                query = "select id from " + str(tableName) + " where " + str(colName) + " >= " + str(array[i - 1]) + " and " + str(colName) + " < " + str(array[i]) + ";"
        print "\tQuery: " + str(query)
        cursor.execute(query)
        data = cursor.fetchall()
        total_rows = len(data)
        print "\tTotal rows: " + str(total_rows)
        for row in data:
            #insert to a list
            listID[count] = int(row[0])
            count = count + 1
            index = index + 1
            if(count >= MAX_NUM_ROW_BUCKET_COLUMNAR):
                #build imprints
                buildImprints(listID, count, output)
                #3. Output the imprints to files
                writeImprints(output, foutImprints, foutLength, UNCOMPRESSED)
                count = 0
                numBucket = numBucket + 1
        i = i + 1

    if(count > 0): #last row
        #build last imprints
        buildImprints(listID, count, output)
        writeImprints(output, foutImprints, foutLength, UNCOMPRESSED)
        print "\t\tNumber tuples of last bucket: " + str(count)
        count = 0
        numBucket = numBucket + 1

    print "\tTotal buckets: " + str(numBucket)
    print "\tTotal tuples: " + str(index)
    foutImprints.close()
    foutLength.close()
    print "Total number of buckets: " + str(numBucket) + "!\n"
    print "Finished!"
    return

#TODO: On-going -> write a test to evaluate
# 4. Do the test: Read these output files and do the joining between columns
def isBelong(imprints, value):
    if(imprints != None):
        if(value < imprints[0] or value > imprints[NUM_MILESTONE - 1]):
            return False
        for i in range(NUM_MILESTONE / 2):
            if(value <= imprints[i * 2 + 1] and value >= imprints[i * 2]):
                return True
    return False

def isBelong2(imprints, list_ID):
    if(imprints != None and list_ID != None):
        length = len(list_ID) #list ID is already sorted
        interval = [list_ID[0], list_ID[length - 1]]
        if(list_ID[0] > imprints[NUM_MILESTONE - 1] or list_ID[length - 1] < imprints[0]):
            return False
        if(list_ID[0] >= imprints[0] and list_ID[0] <= imprints[NUM_MILESTONE - 1] and list_ID[1] >= imprints[NUM_MILESTONE - 1]):
            for i in range(length):
                if (list_ID[i] > imprints[NUM_MILESTONE]):
                    return False
                if (isBelong(imprints, list_ID[i])):
                    return True
        elif(list_ID[0] >= imprints[0] and list_ID[1] <= imprints[NUM_MILESTONE - 1]):
            for i in range(length):
                if(isBelong(imprints, list_ID[i])):
                    return True
        elif(list_ID[0] <= imprints[0] and list_ID[1] >= imprints[NUM_MILESTONE - 1]):
            for i in range(length):
                if(list_ID[i] < imprints[0]):
                    continue
                if(list_ID[i] > imprints[NUM_MILESTONE]):
                    return False
                if(isBelong(imprints, list_ID[i])):
                    return True
        elif(list_ID[0] <= imprints[0] and list_ID[1] <= imprints[NUM_MILESTONE - 1]):
            for i in range(length):
                if(list_ID[i] < imprints[0]):
                    continue
                if(isBelong(imprints, list_ID[i])):
                    return True
    return False

def evaluation_1(ImprintsFile, LengthFile, table, column, value1, value2):
    # 1. Do query on the table1 on column1 -> get the list of IDs
    # 1.1 Connect to database
    print "Connecting to the database..."
    try:
        conn = psycopg2.connect("dbname='nyc-taxi-data' user='tonhai' password='TonHai1111'")
        cursor = conn.cursor()
    except:
        print "Error: Cannot connect to the nyc-taxi-data database!"
        return
    print "Connected!"
    # 1.2 Do querying
    print "Do querying..."
    query = "select distinct id from " + str(table) + " where " + str(column) + " > " + str(value1)
    query = query + " and " + str(column) + " < " + str(value2) + " order by id;"
    cursor.execute(query)
    data = cursor.fetchall()
    # 1.3 Loading data (ID) to buffer
    total_rows = len(data)
    list_ID = [0 for x in range(total_rows)]
    for i in range(total_rows):
        list_ID[i] = int(data[i][0]) # already ordered by ID

    # 2. Load imprintsFile2 (with LengthFile2) to RAM
    print "Loading imprintsFile to RAM..."
    totalBuckets = int(math.ceil((float) (MAX_ROW_DB) / (float)(MAX_NUM_ROW_BUCKET_COLUMNAR)))
    #   ImprintsMetaData[totalBuckets][NUM_MILESTONE]
    imprintsMetaData = [[0.0 for x in range(0, NUM_MILESTONE + 1)] for y in range(0, totalBuckets)]
    fin_Imprints = open(ImprintsFile, 'r')
    fin_Length = open(LengthFile, 'r')
    curImp = 0 # Imprints
    curMS = 0 # Milestone
    for line in fin_Imprints:
        if (line.strip() != ''):
            curValue = int(line)
            imprintsMetaData[curImp][curMS] = curValue
            curMS += 1
            if(curMS >= NUM_MILESTONE):
                curMS = 0
                curImp += 1
    fin_Length.close()
    fin_Imprints.close()
    # 3. Scan for list of buckets of table2.
    print "Scanning for the list of buckets..."
    result = []
    count = 0
    print len(list_ID)
    print totalBuckets
    #for id in list_ID:
    #    count += 1
    #    if(count % 1000 == 0):
    #        print count
    #    for i in range(totalBuckets):
    #        if(imprintsMetaData[i][NUM_MILESTONE] <= 0.0 and isBelong(imprintsMetaData[i], id)):
    #            imprintsMetaData[i][NUM_MILESTONE] = 1
    #            result.append(i)

    for i in range(totalBuckets):
        if(i % 1000 == 0):
            print i
        if(isBelong2(imprintsMetaData[i], list_ID)):
            result.append(i)
    # 3.1 Printing output ...
    print "Output: "
    print "Total number of IDs (in " + str(column) + "): " + str(len(list_ID)) + "!"
    print "Total number of buckets: " + str(len(result))
    #print "Results: "
    #print result
    print "===================================="
    return
# 5. Report the amount of data accessed.
# 6. ...

if __name__ == '__main__':
    #build_imprints_from_DB('trips_test_imprints.dat', 'length_test.dat', 0)
    #build_imprints_from_DB('trips_proj5_imprints.dat', 'length_Info5.dat', 0)
    #build_imprints_from_DB('trips_proj6_imprints.dat', 'length_Info6.dat', 1)
    #build_imprints_from_DB('trips_proj7_imprints.dat', 'length_Info7.dat', 2)
    #Call evaluation function to test imprints (extension)
    evaluation_1("imprints/trips_proj7_imprints.dat", "imprints/length_Info7.dat", "trips5", "trip_distance", 0.13, 0.15)
    print "-----------------------\n"
