#!/usr/bin/python
# -*- coding:utf-8 -*-
import argparse
import time
import os
import MySQLdb


def main():
    path = '/dataPool/map/2015cabGPS_splitpath/'
    os.chdir(path)
    dirnames = os.listdir(path)
    for dirname in dirnames:
        if len(os.listdir(path + dirname)) == 0:
            os.rmdir(dirname)


if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
