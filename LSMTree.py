#!/usr/bin/python

import os
import shutil
#Constants
LSM_RATIO = 4
LSM_RUN_LENGTH = 100
LSM_DB = 'lsm_db'
LSM_METADATA = 'lsm_metadata.dat'
MIN_KEY = 0 #Changing key type, should change MIN_KEY, MAX_KEY as well
MAX_KEY = 999999999
#Levelled LSM-Tree
class LSMTree():
    def __init__(self, in_db_path = LSM_DB, l_ratio = LSM_RATIO):
        self.num_level = 1
        self.ratio = l_ratio
        self.components = []
        self.db_path = in_db_path
        self.metadata = LSM_METADATA
        mkdir_p(os.path.join(self.db_path))
        cleanup(os.path.join(self.db_path))
        self.components.append(LSMComponent(l_ratio))
        return

    def __init__(self, db_path, n_level, l_ratio):
        self.num_level = n_level
        self.ratio = l_ratio
        self.db_path = in_db_path
        self.metadata = LSM_METADATA
        self.components = []
        for i in range(self.num_level):
            if(i == 0):
                mkdir_p(os.path.join(self.db_path))
                cleanup(os.path.join(self.db_path))
            else:
                mkdir_p(os.path.join(self.db_path, str(i)))
            self.components.append(LSMComponent(i, l_ratio))

    def insert(self, entry):
        idx = self.components[0].count
        while (idx > 0 and entry.key <= self.components[0].datapool[idx - 1].key):
            idx -= 1
        self.components[0].datapool.insert(idx, entry)
        self.components[0].count += 1
        if(self.components[0].count == self.components[0].capacity):
            if(self.num_level == 1):
                self.components.append(LSMComponent(1, self.ratio))
                #self.components[1].zonemaps.append(init_aZoneMap())
                self.level += 1
            self.merge(0, 1)
            self.components[0].datapool.clear()
            self.components[0].count = 0
        return

    def search_point(self, k_value):
        #TODO
        return

    def search_range(self, k_range):
        #TODO
        return

    def write_metadata(self):
        #<Number of levels> \t <ratio> \t <db_path> \t <metadata> \n
        #<Component1> \t <count> \t <run: min; max> \t <min; max> ... <min; max> \n
        #<Componentn> \t <count> \t <run: min; max> \t <min; max> ... <min; max> \n
        #<Component0> \t <count> \n
        #key;value
        #...
        #key;value
        fout = open(self.metadata, 'w')
        str = str(self.num_level) + '\t' + str(self.ratio) + '\t' +
                str(self.db_path) + '\t' + str(self.metadata) + '\n'
        fout.write(str)
        for i in range(self.num_level):
            str = 'Level ' + str(i) + ':\t'
            for j in range(self.components[i].count):
                str += '<' + str(self.components[i].zonmaps[j][0]) + ';' +
                        str(self.components[i].zonmaps[j][1]) + '>' + '\t'
            str += '\n'
            fout.write(str)
        str = 'Level 0:\t' + str(self.components[0].count) + '\n'
        fout.write(str)
        for i in range(self.components[0].count):
            str = entrytoStr(self.components[0].datapool[i]) + '\n'
            fout.write(str)
        fout.close()
        return

    def load_metadata(self):
        #TODO
        return

    def merge(self, level1, level2):
        buf = []
        if(level1 == 0):
            num_run1 = 1
        else:
            num_run1 = self.components[level1].count
        num_run2 = self.components[level2].count
        ptr1 = LSM_RUN_LENGTH
        ptr2 = LSM_RUN_LENGTH
        ptr = 0
        cnt_run1 = 0
        cnt_run2 = 0
        cnt_run = 0
        stop1 = False
        stop2 = False
        cur_zm = init_aZoneMap()
        zonemaps = []
        while((not stop1) and (not stop2)):
            #Get data1
            if(ptr1 == LSM_RUN_LENGTH):
                if(cnt_run1 < num_run1):
                    if(level1 == 0):
                        data1 = self.components[0].datapool
                    else:
                        data1, mgs1 = self.components[level1].read_run(cnt_run1)
                    cnt_run1 += 1
                    ptr1 = 0
                else:
                    stop1 = True
                    break
            #Get data2
            if(ptr2 == LSM_RUN_LENGTH):
                if(cnt_run1 < num_run2):
                    data2, mgs2 = self.components[level2].read_run(cnt_run2)
                    cnt_run2 += 1
                    ptr2 = 0
                else:
                    stop2 = True
                    break
            #merge
            while (ptr1 < LSM_RUN_LENGTH and ptr2 < LSM_RUN_LENGTH):
                if(data1[ptr1].key < data2[ptr2].key):
                    buf[ptr] = data1[ptr1]
                    ptr1 += 1
                else:
                    buf[ptr] = data2[ptr2]
                    ptr2 += 1
                if(buf[ptr] > cur_zm[1]):
                    cur_zm[1] = buf[ptr]
                ptr += 1
                if(ptr == LSM_RUN_LENGTH):
                    self.components[level2].write_run_tmp(buf, cnt_run)
                    zonemaps.insert(cnt_run, cur_zm[:])
                    clear_aZoneMap(cur_zm)
                    cnt_run += 1

        if(stop1):#no more run on level1
            #handle the current buf and data2
            if(ptr2 < LSM_RUN_LENGTH):
                while(ptr2 < LSM_RUN_LENGTH):
                    buf[ptr] = data2[ptr2]
                    if(buf[ptr] > cur_zm[1]):
                        cur_zm[1] = buf[ptr]
                    ptr += 1
                    ptr2 += 1
                self.components[level2].write_run_tmp(buf, cnt_run)
                zonemaps.insert(cnt_run, cur_zm[:])
                clear_aZoneMap(cur_zm)
                cnt_run += 1
            #Copy the rest of level2 to level2_tmp
            while (cnt_run2 < num_run2):
                self.components[level2].org_to_tmp(level2, cnt_run2, level2, cnt_run)
                zonemaps.insert(cnt_run, self.components[level2].zonemaps[cnt_run2][:])
                cnt_run2 += 1
                cnt_run += 1

        if(stop2):#no more run on level2
            #handld the current buf and data1
            if(ptr1 < LSM_RUN_LENGTH):
                while (ptr1 < LSM_RUN_LENGTH):
                    buf[ptr] = data1[ptr1]
                    ptr += 1
                    ptr1 += 1
                self.components[level2].write_run_tmp(buf, cnt_run)
                cnt_run += 1
            #Copy the rest of level1 to level2_tmp
            while (cnt_run1 < num_run1):
                self.components[level2].org_to_tmp(level1, cnt_run1, level2, cnt_run)
                zonemaps.insert(cnt_run, self.components[level1].zonemaps[cnt_run1][:])
                cnt_run1 += 1
                cnt_run += 1

        #2. Remove level2 and rename level2_tmp
        self.components[level2].clear_org()
        self.components[level2].rename_tmp()
        del self.components[level2].zonemaps[:]
        self.components[level2].count = cnt_run
        for i in range(cnt_run):
            self.components[level2].zonmaps.append(zonemaps[i][:])
        #Clear level1
        self.components[level1].count = 0
        del self.components[level1].zonemaps[:]

        #3. Call higher merge if count reaches to its limitation
        if(self.components[level2].count == self.capacity):
            if(self.num_level - 1 == level2):
                self.components.append(LSMComponent(level2 + 1, self.ratio))
                self.num_level += 1
            self.merge(level2, level2 + 1)
        return

class LSMComponent():
    def __init__(self, l_ratio = LSM_RATIO):
        self.level = 0
        self.ratio = l_ratio
        #self.isCache = True #True: this component is C0 (in Memory)
        #                    #False: this component is Cn (n>0, on-disk)
        self.capacity = LSM_RUN_LENGTH * pow(self.ratio, self.level)
        self.count = 0  #number of entries in C0
                        #number of runs in Cn (n>0)
        self.zonemaps = []
        self.bloomfilters = []
        if(self.level == 0):
            self.datapool = []  #Only avalabile if component is C0
        else:
            self.fences = []    #fences is Empty if component is C0
                                #otherwise, fences is a list of pointers
                                # to on-disk runs
        dir = os.path.join('lsm_db')
        self.files = []     #files is Empty if components is C0
                            #otherwise, files is a list of file pointers

    def __init__(self, c_level, l_ratio):
        self.level = c_level
        self.ratio = l_ratio
        #self.isCache = True if (in_level == 0) else False
        self.capacity = LSM_RUN_LENGTH * pow(self.ratio, self.level)
        self.count = 0  #number of entries in C0
                        #number of runs in Cn (n>0)
        if(self.level == 0):
            self.datapool = []  #Only avalabile if component is C0
        else:
            self.fences = []    #fences is Empty if component is C0
                                #otherwise, fences is a list of pointers
                                # to on-disk runs

    def read_run(self, run_pos):
        if(self.isCache):
            return (None, "Error 0001: No read file on Cache component!")
        if(run_pos >= self.count):
            return (None, "Error 0003: (OverRead) No data to read!")
        fn = os.path.join(str(self.db_path), str(self.level), str(run_pos))
        fin = open(fn, 'r')
        for line in fin:
            entry = strtoEntry(line)
            rs.append(entry)
        #rs = fin.read(LSM_RUN_LENGTH)
        fin.close()
        return (rs, "Succeed!")

    def write_run(self, entries, run_pos):
        if(self.isCache):
            return (False, "Error 0002: No write file on Cache component!")
        fn = os.path.join(str(self.db_path), str(self.level), str(run_pos))
        fout = open(fn, 'w')
        for i in range(len(entries)):
            str = entrytoStr(entries)
            fout.write(str + '\n')
        #fout.write(b_data)
        fout.close()
        self.count += len(entries)
        return (True, "Succeed!")

    def write_run_tmp(self, entries, run_pos):
        if(self.isCache):
            return (False, "Error 0002: No write file on Cache component!")
        fn = os.path.join(str(self.db_path), str(self.level) + '_tmp', str(run_pos))
        fout = open(fn, 'w')
        for i in range(len(entries)):
            str = entrytoStr(entries)
            fout.write(str + '\n')
        #fout.write(b_data)
        fout.close()
        self.count += len(entries)
        return (True, "Succeed!")

    def org_to_tmp(self, org_lv, org_run, tmp_lv, tmp_run):
        org_fn = os.path.join(str(self.db_path), org_lv, org_run)
        des_fn = os.path.join(str(self.db_path), tmp_lv + '_tmp', tmp_run)
        copyfile(org_fn, des_fn)
        return

    def clear_org(self):
        org_dir = os.path.join(str(self.db_path), str(self.level))
        cleanup(org_dir)
        os.rmdir(org_dir)

    def rename_tmp(self):
        dir = os.path.join(str(self.db_path), str(self.level))
        tmp_dir = os.path.join(str(self.db_path), str(self.level) + '_tmp')
        shutil.move(tmp_dir, dir)




#Helper functions, will be moved to utils.py
def mkdir_p(path):
    try:
        os.makedirs(path)
        return True
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            return False
        else:
            raise

def cleanup(self, pkgdir):
    shutil.rmtree(pkgdir, ignore_errors=True)

def entrytoStr(self, entry):
    rs = str(entry.key) + ';' + str(entry.data)
    return rs

def strtoEntry(self, str):
    rs = str.split(';')
    return [int(rs[0]), rs[1].strip()]

def copyfile(src, des):
    shutil.copy(src, des)

def init_aZoneMap():
    return [MIN_KEY, MIN_KEY]

def clear_aZoneMap(zonemap):
    zonemap[0] = MIN_KEY
    zonemap[1] = MIN_KEY
