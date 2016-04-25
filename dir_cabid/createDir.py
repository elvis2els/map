#!/usr/bin/python
#-*-coding:UTF-8-*-
import argparse
import os
import time
import MySQLdb

parser = argparse.ArgumentParser(description="using to create dir")
parser.add_argument('path', help="input path, output path", nargs=2)
args = parser.parse_args()


def main():
    inputPath = args.path[0]
    outputRoot = args.path[1]
    for root, dirs, days in os.walk(inputPath):
        for day in days:
            f = open(inputPath + day, 'r')
            cabids = f.readlines()
            f.close()
            for cabid in cabids:
                cabid = cabid.strip('\n')
                if not os.path.isdir(outputRoot + cabid):
                    os.mkdir(outputRoot + cabid)

if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
