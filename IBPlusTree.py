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

class IBPlusTree():
    def __init__(self):
        self.rootNode = None
        self.ibTree = IBTree()
        self.evaluation = Evaluation()
        self.ibPlusDataBase = "ibPlusTreeDB.dat"
        self.ibPlusMetaData = "ibPlusTreeMD.dat"
        self.ibPlusBuffer = IBPlusDataBuffer(self.ibPlusDataBase)

        self.cntTuples = 0 #Count number of tuples for purpose of reshape
        self.curLoad = Constants.INIT_LOAD
        self.cntInterval = 0

        # WriteMetaData
        self.nextPositionIndex = -1
        self.pointerStack = Stack()

    #def __init__(self, IBTree):
    #    self.rootNode = None
    #    self.ibTree = IBTree
    #    self.evaluation = Evaluation()
    #    self.ibPlusDataBase = "ibPlusTreeDB.dat"
    #    self.ibPlusMetaData = "ibPlusTreeMD.dat"
    #    self.ibPlusBuffer = IBPlusDataBuffer(self.ibPlusDataBase)
    #    #WriteMetaData
    #    self.nextPositionIndex = -1
    #    self.pointerStack = Stack()

    def setInfo(self, _ibPlustDataBase, _ibPlusMetaData):
        self.ibPlusDataBase = _ibPlustDataBase
        self.ibPlusMetaData = _ibPlusMetaData
        self.ibPlusBuffer.ibPlusDataBase = self.ibPlusDataBase
        return

    def setIBTree(self, _ibTree):
        self.ibTree = _ibTree
        return
    ##############<Insert>##############
    def insertTuple(self, _key, _tuple):
        result = Constants.ERROR
        if (self.rootNode is None):
            str = raw_input('IB+-Tree is Null! \nWe recommend the users to build IB+-Tree structure by calling copyStructure(IBTree) before inserting data into the tree! \nAre you sure to insert data into an Empty IB+-Tree? (y/n)')
            if(str == 'y'):
                self.rootNode = newIBPlusNode(True)
            else:
                return result
        result = self.insertTupleRec(self.rootNode, _key, _tuple)
        #if(result == Constants.OVERFLOW):
        #    #Insert new node, create new root and return success
        #    result = Constants.SUCCESS
        self.cntTuples += 1
        ##if(self.cntTuples == Constants.RESHAPE_LIMIT):
        ##    self.reShape() # Reshape IBPlusTree
        ##    self.cntTuples = 0
        return result

    def insertTupleRec(self, curNode, _key, _tuple):
        #if(_key <= 0.28):
        #    pdb.set_trace()
        #Recursively insert a tuple into IBPlus-Tree
        result = Constants.ERROR
        if (curNode is None):
            print "####################<Error code: 10>#######################"
            return result
        goodPlace = findGoodPlace(curNode, _key, curNode.isLeaf())
        if(curNode.isLeaf()): #Leaf node
            #print "####################<Leaf>#######################"
            tuple = self.ibPlusBuffer.createTuple(_key, _tuple)
            if(tuple is None):
                print "####################<Warning code: 11>#######################"
                self.group()
                tuple = self.ibPlusBuffer.createTuple(_key, _tuple)
            curNode.data[goodPlace][curNode.count[goodPlace]] = tuple
            curNode.count[goodPlace] += 1

            curNode.dis[goodPlace] += 1
            result = Constants.SUCCESS
            if(curNode.interval[goodPlace][Constants.HIGH] < _key):
                curNode.interval[goodPlace][Constants.HIGH] = _key
                result = Constants.SUCCESS_UPDATE_MAX
            if(curNode.count[goodPlace] >= Constants.NUM_ROW_PER_BUCKET):
                result = self.move(curNode, goodPlace) #move this full bucket to flushing area
                curNode.count[goodPlace] = 0
                curNode.interval[goodPlace][Constants.HIGH] = curNode.interval[goodPlace][Constants.LOW]
                if(result == Constants.SUCCESS):
                    result = Constants.SUCCESS_UPDATE_MAX
        else: #Non-Leaf node
            result = self.insertTupleRec(curNode.pointer[goodPlace], _key, _tuple)
            if(result == Constants.SUCCESS_UPDATE_MAX):
                for i in range(0, curNode.length):
                    if(curNode.max[goodPlace] < curNode.pointer[goodPlace].interval[i][Constants.HIGH]):
                        curNode.max[goodPlace] = curNode.pointer[goodPlace].interval[i][Constants.HIGH]
                result = Constants.SUCCESS
        return result

    ##############<Search>##############
    def search(self, _outputTuples, _interval):
        #Search all tuples whose intervals intersect with the given interval (_interval)
        result = self.searchRec(self.rootNode, _outputTuples, _interval)
        return result

    def search(self, _outputTuples, _outputBucket, _interval):
        #Search all tuples and buckets whose intervals intersect with the given interval (_interval)
        result = self.flush()
        if(result == Constants.ERROR):
            return result
        result = self.ibTree.search(_outputBucket, _interval)
        if(result == Constants.ERROR):
            return result
        result = self.searchRec(self.rootNode, _outputTuples, _interval)
        return result

    def searchRec(self, curNode, _outputTubles, _interval):
        #search recursively all tuples bucketIDs whose intervals intersect with the given interval (_interval)
        result = Constants.SUCCESS
        if (curNode is None):
            result = Constants.ERROR
            return result
        if(curNode.isLeaf()):
            for i in range(0, curNode.length):
                if(_interval[Constants.HIGH] < curNode.interval[i][Constants.LOW]):
                    break
                if(intersect(_interval, curNode.interval[i])):
                    for j in range(0, curNode.count[i]):
                        if(inside(_interval, curNode.data[i][j].key)):
                            _outputTubles.results.append(curNode.data[i][j].data)
                            #print "Position: " + str(i) + " | " + str(j)
                            #pdb.set_trace()
        else:
            for i in range(0, curNode.length):
                if(result == Constants.ERROR):
                    return result
                if ((_interval[Constants.LOW] <= curNode.max[i]) & (curNode.pointer[i] is not None)):
                    #print i
                    result = self.searchRec(curNode.pointer[i], _outputTubles, _interval)
                if(_interval[Constants.HIGH] < curNode.interval[i][Constants.LOW]):
                    return result
            #Last child
            #print "lastchild"
            if((_interval[Constants.LOW] <= curNode.max[curNode.length]) & (curNode.pointer[curNode.length] is not None)):
                result = self.searchRec(curNode.pointer[curNode.length], _outputTubles, _interval)
        return result

    def searchImprintsAll(self, _outputTuples, _outputBucket, _interval):
        #Search all tuples and buckets whose intervals intersect with the given interval (_interval)
        result = self.flush()
        if(result == Constants.ERROR):
            return result
        result = self.ibTree.searchImprints(_outputBucket, _interval)
        if(result == Constants.ERROR):
            return result
        result = self.searchRecImprints(self.rootNode, _outputTuples, _interval)
        return result

    def searchImprints(self, _outputTuples, _interval):
        #Search all tuples whose intervals intersect with the given interval (_interval)
        result = self.searchRecImprints(self.rootNode, _outputTuples, _interval)
        return result

    def searchRecImprints(self, curNode, _outputTubles, _interval):
        #search recursively all tuples bucketIDs whose intervals intersect with the given interval (_interval)
        result = Constants.SUCCESS
        if (curNode is None):
            result = Constants.ERROR
            return result
        if(curNode.isLeaf()):
            for i in range(0, curNode.length):
                if(compareImprints(_interval[Constants.HIGH], curNode.interval[i][Constants.LOW]) < 0):
                    break
                if(intersectImprints(_interval, curNode.interval[i])):
                    for j in range(0, curNode.count[i]):
                        if(insideImprints(_interval, curNode.data[i][j].key)):
                            _outputTubles.results.append(curNode.data[i][j].data)
                            #print "Position: " + str(i) + " | " + str(j)
                            #pdb.set_trace()
        else:
            for i in range(0, curNode.length):
                if(result == Constants.ERROR):
                    return result
                if ((compareImprints(_interval[Constants.LOW], curNode.max[i]) <= 0) & (curNode.pointer[i] is not None)):
                    #print i
                    result = self.searchRecImprints(curNode.pointer[i], _outputTubles, _interval)
                if(compareImprints(_interval[Constants.HIGH], curNode.interval[i][Constants.LOW]) < 0):
                    return result
            #Last child
            #print "lastchild"
            if((compareImprints(_interval[Constants.LOW], curNode.max[curNode.length]) <= 0) & (curNode.pointer[curNode.length] is not None)):
                result = self.searchRecImprints(curNode.pointer[curNode.length], _outputTubles, _interval)
        return result
    ##############<Group>##############
    def group(self):
        #Grouping data in all leaf nodes
        # Grouping:
        #   + 1. Combining data in all entries of a leaf node to create full buckets
        #   + 2. Moving these buckets to flushing area
        #   + 3. Flush these buckets into database (Automatically be called by moveBucket(tuples) function
        result = Constants.ERROR
        if(self.rootNode is None):
            return result
        result = self.groupRec(self.rootNode)
        return result

    def groupRec(self, curNode):
        result = Constants.ERROR
        if (curNode is None):
            return result
        result = Constants.SUCCESS
        if (curNode.isLeaf()): #Leaf node
            tuples = [None for x in range(0, Constants.NUM_ROW_PER_BUCKET)]
            index = 0
            totalEntries = 0
            for i in range(0, curNode.length):
                totalEntries += curNode.count[i]
            totalBuckets = totalEntries / Constants.NUM_ROW_PER_BUCKET
            i = 0
            while (totalBuckets > 0):
                length = curNode.count[i]
                for j in range (0, length):
                    tuples[index] = curNode.data[i][curNode.count[i] - j - 1]
                    index += 1
                    curNode.data[i][curNode.count[i] - j - 1] = None
                    curNode.count[i] -= 1
                    if(index == Constants.NUM_ROW_PER_BUCKET):
                        self.ibPlusBuffer.moveBucket(tuples)
                        index = 0
                        totalBuckets -= 1
                        if (totalBuckets == 0):
                            break
                i += 1
        else: #Non-Leaf node
            for i in range(0, curNode.length + 1):
                if (result == Constants.ERROR):
                    break
                result = self.groupRec(curNode.pointer[i])
        return result

    ##############<reShape the IB+-Tree>##############
    def reShape(self):
        # DONE:
        # I. build the sibling link for the leaf-level ---> OK (linkLeaves)
        # II. Build a function to rebuild the tree from leaves ---> Ok (reBuild)
        # III. Extend the node to store the distribution ---> OK (dis attr.)
        #   1. How would you store the distribution?
        #   with distribution attribute in nonleaf-node
        #   increase whenever a tuple is inserted
        # IV. Build a function to record the current and on-going ditribution
        # as well as periorly launch the reShape function ---> OK (reShape)
                #1. record the data distribution? ---> OK (see above)
                #2. Accumulate the current distribution to the on-going distribution
                # Add (i) ongoing attribute and (ii) load attribute
                #3. Global distribution

        if(self.rootNode is None):
            return Constants.ERROR
        leftLeaf = self.rootNode
        while(not leftLeaf.isLeaf()):
            leftLeaf = leftLeaf.pointer[0]
        #Phase 1: Accumulate the current distribution to on-going distribution
        while (leftLeaf is not None):
            for i in range(0, len(leftLeaf.interval)): #(0, MAX_NUM_L_ENTRY)
                leftLeaf.ongoing[i] = (leftLeaf.ongoing[i] * leftLeaf.oLoad[i] +
                leftLeaf.dis[i] * Constants.RESHAPE_LIMIT)/(leftLeaf.oLoad[i] + Constants.RESHAPE_LIMIT)
                leftLeaf.oLoad[i] += Constants.RESHAPE_LIMIT
                leftLeaf.dis[i] = 0
            leftLeaf = leftLeaf.sibling
        #Phase 2: Commit the on-going milestones to global milestones, tune
        #   the global milestones if necessary
        leftLeaf = self.rootNode
        while(not leftLeaf.isLeaf()):
            leftLeaf = leftLeaf.pointer[0]
        while(leftLeaf is not None):
            for i in range(0, len(leftLeaf.interval)): #(0, MAX_NUM_L_ENTRY)
                if(self.checkInterval(leftLeaf, i) >= Constants.PHI_MAX):
                    leftLeaf.gDis[i] = (leftLeaf.gDis[i] * leftLeaf.gLoad[i] +
                    leftLeaf.ongoing[i] * leftLeaf.oLoad[i])/(leftLeaf.gDis[i] + leftLeaf.oLoad[i])
                    leftLeaf.gLoad[i] += leftLeaf.oLoad[i]
                    leftLeaf.oLoad[i] = 0
                    leftLeaf.ongoing[i] = 0
                    self.splitInterval(leftLeaf, i)
                    i += 1
                elif(self.checkInterval(leftLeaf, i) <= Constants.PHI_MIN):
                    leftLeaf.gDis[i] = (leftLeaf.gDis[i] * leftLeaf.gLoad[i] +
                    leftLeaf.ongoing[i] * leftLeaf.oLoad[i])/(leftLeaf.gDis[i] + leftLeaf.oLoad[i])
                    leftLeaf.gLoad[i] += leftLeaf.oLoad[i]
                    leftLeaf.oLoad[i] = 0
                    leftLeaf.ongoing[i] = 0
                    self.mergeInterval(leftLeaf, i)
            leftLeaf = leftLeaf.sibling
        #Rebuild the tree from leaves
        result = self.reBuild()
        return result

    def splitInterval(self, leaf, pos):
        leaf.interval.insert(pos, [0.0, 0.0])
        leaf.interval[pos][Constants.LOW] = leaf.interval[pos + 1][Constants.LOW]
        leaf.interval[pos][Constants.HIGH] = (leaf.interval[pos + 1][Constants.LOW] + leaf.interval[pos + 1][Constants.HIGH])/2
        leaf.interval[pos + 1][Constants.LOW] = leaf.interval[pos][Constants.HIGH]
        #leaf.interval[pos + 1][Constants.HIGH] = leaf.interval[pos + 1][Constants.HIGH]

        leaf.data.insert(pos, [None for x in range(0, Constants.NUM_ROW_PER_BUCKET)])
        leaf.count.insert(pos, 0)
        leaf.dis.insert(pos, 0.0)

        leaf.ongoing.insert(pos, 0.0)
        leaf.ongoing[pos] = leaf.ongoing[pos + 1] / 2
        leaf.ongoing[pos + 1] = leaf.ongoing[pos + 1] / 2
        leaf.oLoad.insert(pos, 0.0)
        leaf.oLoad[pos] = leaf.oLoad[pos + 1] / 2
        leaf.oLoad[pos + 1] = leaf.oLoad[pos + 1] / 2

        leaf.gDis.insert(pos, 0.0)
        leaf.gDis[pos] = leaf.gDis[pos + 1] / 2
        leaf.gDis[pos + 1] = leaf.gDis[pos + 1] / 2
        leaf.gLoad.insert(pos, 0.0)
        leaf.gLoad[pos] = leaf.gLoad[pos + 1] / 2
        leaf.gLoad[pos + 1] = leaf.gLoad[pos + 1] / 2
        return Constants.SUCCESS

    def mergeInterval(self, leaf, pos):
        sel = pos
        if (pos == 0):
            sel = pos + 1
        elif (pos == Constants.MAX_NUM_L_ENTRY - 1):
            sel = pos - 1
        else:
            if(leaf.gDis[pos - 1]/leaf.gLoad[pos - 1] > leaf.gDis[pos + 1]/leaf.gLoad[pos + 1]):
                sel = pos + 1
            else:
                sel = pos - 1
        if(sel > pos):
            leaf.interval[sel][Constants.LOW] = leaf.interval[pos][Constants.LOW]
        else:
            leaf.interval[sel][Constants.HIGH] = leaf.interval[pos][Constants.HIGH]
        leaf.interval.pop(pos)
        leaf.data.pop(pos)
        leaf.count.pop(pos)
        leaf.dis.pop(pos)

        leaf.ongoing[sel] += leaf.ongoing[pos]
        leaf.ongoing.pop(pos)
        leaf.oLoad[sel] += leaf.oLoad[pos]
        leaf.oLoad.pop(pos)
        leaf.gDis[sel] += leaf.gDis[pos]
        leaf.gDis.pop(pos)
        leaf.gLoad[sel] += leaf.gLoad[pos]
        leaf.gLoad.pop(pos)
        return Constants.SUCCESS

    def checkInterval(self, node, pos):
        avg_dis = self.curLoad/self.cntInterval
        result = (node.ongoing[pos] * node.oLoad[pos] +
        node.gDis[pos] * node.gLoad[pos]) / (self.curLoad * avg_dis)
        return result

    #Link all leaves together with sibling link
    def linkLeaves(self):
        if(self.rootNode is None):
            return Constants.ERROR
        ##pdb.set_trace()
        queue = []
        queue.append(self.rootNode)
        leftSibling = None
        while(len(queue) > 0):
            curNode = queue.pop()
            ##pdb.set_trace()
            if(curNode.isLeaf()):
                if(leftSibling is not None):
                    leftSibling.sibling = curNode
                leftSibling = curNode
            else:
                for i in range(0, curNode.length):
                    queue.append(curNode.pointer[i])
        return Constants.SUCCESS

    def countNumInterval(self):
        if(self.rootNode is None):
            return Constants.ERROR
        count = 0
        leftLeaf = self.rootNode
        while(not leftLeaf.isLeaf()):
            leftLeaf = leftLeaf.pointer[0]
        while (leftLeaf is not None):
            count += len(leftLeaf.interval)
            leftLeaf = leftLeaf.sibling
        self.cntInterval = count
        return Constants.SUCCESS

    def initGlobalDistribution(self):
        if(self.rootNode is None):
            return Constants.ERROR
        count = 0
        leftLeaf = self.rootNode
        while(not leftLeaf.isLeaf()):
            leftLeaf = leftLeaf.pointer[0]
        avg_dis = Constants.INIT_LOAD / self.cntInterval
        while (leftLeaf is not None):
            for i in range(0, len(leftLeaf.interval)):
                leftLeaf.gDis[i] = avg_dis
            leftLeaf = leftLeaf.sibling
        return Constants.SUCCESS
    #Rebuild the IB+-Tree from leaves
    def reBuild(self):
        #1. Go to the leftLeaf of the tree
        if(self.rootNode is None):
            return Constants.ERROR
        leftLeaf = self.rootNode
        while(not leftLeaf.isLeaf()):
            leftLeaf = leftLeaf.pointer[0]
        #2. Build the tree from this leaf
        #2.1 Rebuild the leaves
        leavesQueue = []
        aLeaf = newIBPlusNode(True)
        curIndex = 0
        MaxLen = Constants.MAX_NUM_L_ENTRY
        while(leftLeaf is not None):
            curPos = 0
            curLength = len(leftLeaf.interval)
            while(curPos < curLength):
                if((MaxLen - curIndex) > (curLength - curPos)):
                    copyLeafInterval_Dis(aLeaf, curIndex, leftLeaf, curPos, (curLength - curPos))
                    curIndex += curLength - curPos
                    curPos = curLength #Exit while and go for the next leaf
                else:
                    copyLeafInterval_Dis(aLeaf, curIndex, leftLeaf, curPos, (MaxLen - curIndex))
                    curPos += MaxLen - curIndex
                    curIndex = MaxLen #current leaf is full, add to queue
                    leavesQueue.append(aLeaf)
                    aLeaf = newIBPlusNode(True)
                    curIndex = 0

            leftLeaf = leftLeaf.sibling
        #2.2 Rebuild the rood from leaves
        ##pdb.set_trace()
        curLevel = 0
        curIndex = 0
        MaxLen = Constants.MAX_NUM_NL_ENTRY
        aNode = newIBPlusNode(False)
        curNode = None
        while(len(leavesQueue) > 0):
            curNode = leavesQueue.pop(0)
            if(curNode.level > curLevel):
                ##pdb.set_trace()
                leavesQueue.insert(0, curNode)
                leavesQueue.append(aNode)
                aNode = newIBPlusNode(False)
                curIndex = 0
                curLevel = curNode.level
                continue
            if(curIndex == 0):
                aNode.level = curNode.level + 1
            ##pdb.set_trace()
            aNode.interval[curIndex][Constants.LOW] = curNode.interval[0][Constants.LOW]
            aNode.interval[curIndex][Constants.HIGH] = curNode.interval[len(curNode.interval) - 1][Constants.HIGH]
            aNode.max[curIndex] = curNode.interval[len(curNode.interval) - 1][Constants.HIGH]
            aNode.pointer[curIndex] = curNode
            curIndex += 1
            if(curIndex == Constants.MAX_NUM_NL_ENTRY):
                leavesQueue.append(aNode)
                aNode = newIBPlusNode(False)
                curIndex = 0

        #3. Update the new root
        if(curNode is not None):
            self.rootNode = curNode
            return Constants.SUCCESS
        else:
            return Constants.ERROR

    ##############<Copy IBTree Structure>##############
    def copyStructure(self):
        return self.copyStructure(self.ibTree)

    def copyStructure(self, IBTree):
        result = Constants.ERROR
        if(self.rootNode is not None):
            str = raw_input("IB+-Tree is not Null! All data in the IB+-Tree will be lost! Do you want to continue (y/n)?:")
            if(str != "y"):
                return result
        IBRoot = IBTree.getRootNode()
        self.rootNode = newIBPlusNode(IBRoot.isLeaf)
        result = self.copyStructureRec(self.rootNode, IBRoot)
        #set the smallest value for the IB+-Tree
        notFound = True
        tempNode = self.rootNode
        while(notFound):
            if(tempNode.isLeaf()):
                tempNode.interval[0][Constants.LOW] = Constants.MIN_DISTANCE
                notFound = False
            else:
                tempNode = tempNode.pointer[0]
        #Link the leaves together
        if (result != Constants.ERROR):
            result = self.linkLeaves()
        if (result != Constants.ERROR):
            result = self.countNumInterval()
        #Initialize the global distribution
        if(result != Constants.ERROR):
            result = self.initGlobalDistribution()
        return result

    def copyStructureRec(self, curNode, IBNode):
        result = Constants.ERROR
        if(IBNode is None):
            return result
        curNode.length = IBNode.length
        curNode.level = IBNode.level
        for i in range(0, IBNode.length):
            curNode.interval[i][Constants.LOW] = IBNode.interval[i][Constants.LOW]
            curNode.interval[i][Constants.HIGH] = IBNode.interval[i][Constants.LOW]
            #if (i < IBNode.length - 1):
            #    curNode.interval[i][Constants.HIGH] = IBNode.interval[i + 1][Constants.LOW]
            #else:
            #    curNode.interval[i][Constants.HIGH] = Constants.MAX_DISTANCE
        result = Constants.SUCCESS
        if(IBNode.isLeaf == False):
            for i in range(0, IBNode.length):
                curNode.pointer[i] = newIBPlusNode(IBNode.pointer[i].isLeaf)
                result = self.copyStructureRec(curNode.pointer[i], IBNode.pointer[i])
                if(result == Constants.ERROR):
                    return result
            #Last child
            curNode.pointer[IBNode.length] = newIBPlusNode(IBNode.pointer[IBNode.length].isLeaf)
            result = self.copyStructureRec(curNode.pointer[IBNode.length], IBNode.pointer[IBNode.length])
        return result

    ##############<Print>##############
    numPrint = 0 #Note: this variable is only used for testing, it should be commented
    def printIBPlusTree(self, printData):
        #print IB+-Tree
        result = Constants.ERROR
        if(self.rootNode is None):
            return result
        self.numPrint = 0
        if(printData):
            result = self.printAllIBPlusTreeRec(self.rootNode, "", 0)
        else:
            result = self.printInfoIBPlusTreeRec(self.rootNode, "", 0)
        print "=================================== Number of elements: " + str(self.numPrint) + " ==========================================="
        return result

    def printInfoIBPlusTreeRec(self, curNode, _strLable, _level):
        #print all info in IB+-Tree recursively
        result = Constants.ERROR
        if(curNode is None):
            print "++++++++++++++++++++++++++++<ERROR CODE: 0>++++++++++++++++++++++++++++++++"
            return result
        strLable = ""
        if(_strLable != ""):
            strLable = _strLable + "." + str(_level)
        else:
            strLable = str(_level)
        if(curNode.isLeaf()):
            print "Leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t" + str(curNode.count[i]) + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t #: " + str(i)
                self.numPrint += 1
        else:
            print "Non-leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t Max: " + str(curNode.max[i])
                self.numPrint += 1
                result = self.printInfoIBPlusTreeRec(curNode.pointer[i], strLable, i)
                if(result == Constants.ERROR):
                    print "++++++++++++++++++++++++++++<ERROR CODE: 1>++++++++++++++++++++++++++++++++"
                    return result
            #print last child
            print strLable + "\t" + "Last child" + "\t [" + "Low" + ", " + "High" + "]" + "\t Max: " + str(curNode.max[curNode.length])
            result = self.printInfoIBPlusTreeRec(curNode.pointer[curNode.length], strLable, curNode.length)
            if(result == Constants.ERROR):
                print "++++++++++++++++++++++++++++<ERROR CODE: 2>++++++++++++++++++++++++++++++++"
                return result
        result = Constants.SUCCESS
        return result

    def printAllIBPlusTreeRec(self, curNode, _strLable, _level):
        #print All data and info in IB+-Tree recursively
        result = Constants.ERROR
        if(curNode is None):
            print "++++++++++++++++++++++++++++<ERROR CODE: 0>++++++++++++++++++++++++++++++++"
            return result
        strLable = ""
        if(_strLable != ""):
            strLable = _strLable + "." + str(_level)
        else:
            strLable = str(_level)
        if(curNode.isLeaf()):
            print "Leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t" + str(curNode.count[i]) + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t #: " + str(i)
                for j in range(0, curNode.count[i]):
                    # Print data
                    print "Data: ", curNode.data[i][j].data
                self.numPrint += 1
        else:
            print "Non-leaf level"
            for i in range(0, curNode.length):
                print strLable + "\t [" + str(curNode.interval[i][Constants.LOW]) + ", " + str(curNode.interval[i][Constants.HIGH]) + "]" + "\t Max: " + str(curNode.max[i])
                self.numPrint += 1
                result = self.printAllIBPlusTreeRec(curNode.pointer[i], strLable, i)
                if(result == Constants.ERROR):
                    print "++++++++++++++++++++++++++++<ERROR CODE: 1>++++++++++++++++++++++++++++++++"
                    return result
            #print last child
            print strLable + "\t" + "Last child" + "\t [" + "Low" + ", " + "High" + "]" + "\t Max: " + str(curNode.max[curNode.length])
            result = self.printAllIBPlusTreeRec(curNode.pointer[curNode.length], strLable, curNode.length)
            if(result == Constants.ERROR):
                print "++++++++++++++++++++++++++++<ERROR CODE: 2>++++++++++++++++++++++++++++++++"
                return result
        result = Constants.SUCCESS
        return result

    ##############<Write metadata>##############
    def writeMetaData(self):
        self.nextPositionIndex = 0
        self.writeMetaDataRec(self.rootNode, -1)
        return

    def writeMetaDataRec(self, curNode, parentID):
        #Format:
        # NodexIndex    Level   Length  Parent  isLeaf(False)  Interval:[min max];...[min max];    Max: max;...max;"\n"
        # NodexIndex    Level   Length  Parent  isLeaf(True)  Interval:[min max];...[min max];    Count: count;...count;"\n"
        # -1    bucketIndex(1)  rowIndex(1) key content "\n"
        # -1    bucketIndex(1)  rowIndex(2) key content "\n"
        # -1    bucketIndex(3)  rowIndex(1) key content "\n"
        # -1    bucketIndex(3)  rowIndex(2) key content "\n"
        #1. Write current node data
        curIndex = self.nextPositionIndex
        strWrite = str(curIndex) + "\t" + str(curNode.level) + "\t" + str(curNode.length) + "\t"
        strWrite += str(parentID) + "\t" + str(curNode.isLeaf()) + "\t"
        #1.1 interval information
        strWrite += "Interval: "
        for i in range(0, curNode.length):
            strWrite += str(curNode.interval[i][Constants.LOW]) + " " + str(curNode.interval[i][Constants.HIGH]) + "; "
        if(curNode.isLeaf()):
            #1.2 count information
            strWrite += "\t" + "Count: "
            for i in range(0, curNode.length):
                strWrite += str(curNode.count[i]) + "; "
            strWrite += "\n"
            # add the data
            for i in range(0, curNode.length):
                for j in range(0, curNode.count[i]):
                    strWrite += "-1" + "\t" + str(i) + "\t" + str(j) + "\t" + str(curNode.data[i][j].key) + "\t" + str(curNode.data[i][j].data) + "\n"
        else:
            #1.2 max information
            strWrite += "\t" + "Max: "
            for i in range(0, curNode.length):
                strWrite += str(curNode.max[i]) + "; "
            #last max
            strWrite += str(curNode.max[curNode.length]) + "; "
            strWrite += "\n"
        #1.3 write to file
        fout = open(self.ibPlusMetaData, "a+")
        fout.write(strWrite)
        fout.close()
        #1.4 increase next position index
        self.nextPositionIndex += 1

        #2. Recursive function call
        if(not curNode.isLeaf()):
            for i in range(0, curNode.length):
                self.writeMetaDataRec(curNode.pointer[i], curIndex)
            #last child
            self.writeMetaDataRec(curNode.pointer[curNode.length], curIndex)
        return

    def readMetaData(self):
        #Read and rebuild IB+-Tree
        result = Constants.ERROR
        if(self.rootNode is not None):
            str = raw_input("IB+-Tree is not Null! All data in the IB+-Tree will be lost! Do you want to continue (y/n)?:")
            if(str != "y"):
                return result
        #self.rootNode = newIBPlusNode(False)
        self.pointerStack.clear()
        result = self.readMetaDataRec()
        rootP = self.pointerStack.pop()
        while(rootP.index != 0):
            rootP = self.pointerStack.pop()
        self.rootNode = rootP.pointer
        return result

    def readMetaDataRec(self):
        result = Constants.ERROR
        fin = open(self.ibPlusMetaData, "r")
        line = fin.readline()
        while (line != ""):
            values = line.split("\t")
            if(values[0] != '-1'):
                #nodeIndex = values[0]  #level = values[1]  #length = values[2] #parentID = values[3]
                #isLeaf = values[4] #interval = values[5]   #Max/Count = values[6]
                node = newIBPlusNode(values[4]=='True') #create a new node
                node.level = int(values[1])
                #node.length = int(values[2])
                node.length = 0
                length1 = int(values[2])
                parentID = int(values[3])
                listIntervals = values[5].split(':')[1].strip().split(";")
                #pdb.set_trace()
                for i in range(0, length1):
                    data = listIntervals[i].strip().split(' ')
                    node.interval[i][Constants.LOW] = float(data[0])
                    node.interval[i][Constants.HIGH] = float(data[1])
                if(node.isLeaf()): #Leaf-node
                    listCountValues = values[6].split(':')[1].strip().split(';')
                    for i in range(0, length1):
                        node.count[i] = int(listCountValues[i])
                    for i in range(0, length1):
                        #Create new bucket
                        for j in range(0, node.count[i]):
                            line = fin.readline()
                            rowValues = line.split('\t')
                            if(rowValues[0] == '-1'):
                                #Create new row/tuple
                                tuple = self.ibPlusBuffer.createTuple(float(rowValues[3]), str(rowValues[4]))
                                #node.data[i][j].key = tuple.key
                                #node.data[i][j].data = tuple.data
                                node.data[i][j] = tuple
                            else:
                                print "Error: Wrong bucketData format!"
                                return result
                    node.length = int(values[2])

                else:#Non-Leaf node
                    listMaxValues = values[6].split(':')[1].strip().split(";")
                    for i in range(0, length1 + 1):
                        node.max[i] = float(listMaxValues[i])

                nodeP = nodePointer()
                nodeP.pointer = node
                nodeP.index = int(values[0])
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
    ##############<Move>##############
    def move(self, node, place):
        #Move a full bucket entry in data area to flushing area
        #print "Move entry (bucket) at position " + str(place) + " to flushing area!"
        result = Constants.ERROR
        if(node is None):
            return result
        result = self.ibPlusBuffer.moveBucket(node.data[place])
        if(result == Constants.ERROR):
            self.flush()
        return result

    ##############<Flush>##############
    def flush(self):
        #Flush all buckets in Flushing area in IB+-Tree Buffer to Database and release memory
        result = Constants.ERROR
        if (self.ibTree is None):
            return result
        result = self.ibPlusBuffer.flushBuckets(self.ibTree)
        return result

    ##############<Rotate>##############
    def rotate(self):
        return Constants.ERROR

    ##############<Delete>##############
    def delete(self):
        return Constants.ERROR

    ##############<Update>##############
    def update(self):
        return Constants.ERROR


class IBPlusDataBuffer():
    def __init__(self, _ibPlusDataBase):
        self.dataArea = Constants.MAX_DATA_BUFFER
        self.flushingArea = Constants.MAX_FLUSHING_BUFFER
        self.totalSize = self.dataArea + self.flushingArea
        self.availableDA = self.dataArea * 1024 * 2048 #self.dataArea * 1024 * 1024
        #self.listDABuckets = []
        self.usedDA = 0
        self.availableFA = self.flushingArea * 1024 * 2048 #self.flushingArea * 1024 * 1024
        self.listFABuckets = []
        self.usedFA = 0
        self.bucketID = Constants.BUCKET_STARTING_POINT
        self.ibPlusDataBase = _ibPlusDataBase

        #For testing
        #self.createdTuples = 0
        #self.releasedTuples = 0
        #self.movedTuples = 0

    def createTuple(self, key, data):
        if (self.usedDA >= self.dataArea * 1024 * 2048): #self.dataArea * 1024 * 1024
            print "No more space"
            return None
        row = Tuple()
        row.key = key
        copyRowData(row.data, data)
        size = sys.getsizeof(row.key) + sys.getsizeof(row.data)
        #print "Size of a tuple at creation time: " + str(size)
        self.usedDA += size
        self.availableDA -= size
        #For testing
        #self.createdTuples += 1
        return row

    def releaseTuple(self, tuple, isDataArea):
        result = Constants.SUCCESS
        size = sys.getsizeof(tuple.key) + sys.getsizeof(tuple.data)
        #print "Size of a tuple at releasing time: " + str(size)
        if(isDataArea):
            self.usedDA -= size
            self.availableDA += size
            if(self.usedDA < 0):
                print "Error code 4: used space < 0! " + str(self.usedDA)
                result = Constants.ERROR
        else:
            self.usedFA -= size
            self.availableFA += size
            if (self.usedFA < 0):
                print "Error code 5: used space < 0!" + str(self.usedFA)
                result = Constants.ERROR
        del tuple
        #For testing
        #self.releasedTuples += 1
        return result

    def moveBucket(self, tuples):
        result = Constants.ERROR
        if(tuples is None):
            return result
        newBucket = Bucket()
        newBucket.bucketID = self.getNewBucketID()
        for i in range(0, len(tuples)):
            newBucket.tuples.append(tuples[i])
        self.listFABuckets.append(newBucket)
        result = Constants.SUCCESS
        for i in range(len(tuples)):
            result = self.moveTuple(tuples[i])
            tuples[i] = None
        return result

    def moveTuple(self, tuple):
        result = Constants.ERROR
        if (tuple is None):
            return result
        size = sys.getsizeof(tuple.key) + sys.getsizeof(tuple.data)
        self.usedDA -= size
        self.availableDA += size
        self.usedFA += size
        self.availableFA -= size
        if(self.usedFA >= self.flushingArea * 1024 * 2048): #self.flushingArea * 1024 * 1024
            #print "Flushing area is overloaded! Please call flushBuckets(IBTree) function!"
            return result
        result = Constants.SUCCESS
        #For Testing
        #self.movedTuples += 1
        return result

    def flushBuckets(self, IBTree):
        result = Constants.ERROR
        if(IBTree is None):
            return result
        length = len(self.listFABuckets)
        result = Constants.SUCCESS
        if (length == 0):
            return result
        #For all buckets in flushing area
        for i in range(0, length):
            interval = [Constants.MAX_DISTANCE, Constants.MIN_DISTANCE]
            #1. build bucket
            tempBucket = self.listFABuckets.pop()
            for j in range(len(tempBucket.tuples)):
                if(interval[Constants.LOW] > tempBucket.tuples[j].key):
                    interval[Constants.LOW] = tempBucket.tuples[j].key
                if(interval[Constants.HIGH] < tempBucket.tuples[j].key):
                    interval[Constants.HIGH] = tempBucket.tuples[j].key
            #2. insert & index the bucket
            result = self.insertBucketToDB(tempBucket)
            #print "\t [" + str(interval[Constants.LOW]) + ", " + str(interval[Constants.HIGH])+ "]"
            tempBucket.bucketID = result #Assign bucket pointer
            IBTree.insertBucket(interval, tempBucket.bucketID)
            #3. Release bucket
            for j in range(len(tempBucket.tuples)):
                self.releaseTuple(tempBucket.tuples[j], False) # Release tuples in flushing area
        return result

    def insertBucketToDB(self, bucket):
        #insert bucket to database
        #print "Insert bucket <" + str(bucket.bucketID) + "> into database!"
        foutput = open(self.ibPlusDataBase, "a")
        result = foutput.tell() #Start position
        strWrite = ""
        #for i in range(len(bucket.tuples)):
        #    strWrite += str(bucket.tuples[i]) + "\n"
        #Enable these code to test Spearman's correlation
        interval = [Constants.MAX_DISTANCE, Constants.MIN_DISTANCE]
        for i in range(len(bucket.tuples)):
            #strWrite += str(bucket.tuples[i].data) + "\n"
            strWrite += str(bucket.tuples[i].data) + "\t"
        strWrite += "\n"
            #if(interval[Constants.LOW] > bucket.tuples[i].key):
            #    interval[Constants.LOW] = bucket.tuples[i].key
            #if(interval[Constants.HIGH] < bucket.tuples[i].key):
            #    interval[Constants.HIGH] = bucket.tuples[i].key
        #strWrite += "\t [" + str(interval[Constants.LOW]) + ", " + str(interval[Constants.HIGH])+ "]"
        #strWrite += "\n\n\n"
        foutput.write(strWrite)
        foutput.close()
        return result

    def getNewBucketID(self):
        self.bucketID += 1
        return self.bucketID

    def setStartPointBucketID(self, start):
        self.bucketID = start
        return Constants.SUCCESS
