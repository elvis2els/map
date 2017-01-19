#!/usr/bin/python3
# -*- coding:utf-8 -*-
import os
import time

def main():
    srcpath = '/home/elvis/map/2015cabGPS_mapmatch_aivmm/'
    if not os.path.exists(srcpath):
        raise Exception("srcpath is not exists")
    cabids = os.listdir(srcpath)
    maxpos = len(cabids)
    sumcabs = maxpos
    sumtrij = 0
    pos = 0
    while pos < maxpos:
        id = cabids[pos]
        cabidpath = os.path.join(srcpath, id)
        days = os.listdir(cabidpath)
        for day in days:
        	sumtrij += len(os.listdir(os.path.join(cabidpath, day)))
        pos += 1
    print("sumcabs: " + sumcabs)
    print("sumtrij: " + sumtrij)


if __name__ == '__main__':
    begin = time.time()
    main()
    end = time.time()
    print(end - begin)