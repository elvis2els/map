#!/usr/bin/python
# -*- coding:utf-8 -*-
import argparse
import time
import os
import MySQLdb


def main():
    path = '/dataPool/map/achive/'
    for file in filelist:
        print file
    return

if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
