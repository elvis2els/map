#!/usr/bin/python3
# -*- coding:utf-8 -*-
#用于从input文件中生成output文件夹中应该有的文件夹
import os
import argparse
import time

parser = argparse.ArgumentParser(
    description="using to create dir to stroe map matching file")
parser.add_argument(
    'path', help="input path and output path", nargs=2)
args = parser.parse_args()


def main():
    outpath = args.path[1]
    f = open(args.path[0], 'r')
    lines = f.readlines()
    for line in lines:
        dirnames = line.split('/')
        cabidDayPath = os.path.join(outpath,dirnames[4],dirnames[5])
        if not os.path.exists(cabidDayPath):
            os.makedirs(cabidDayPath)


if __name__ == '__main__':
    begin = time.time()
    if args.path:
        main()
    end = time.time()
    print(end - begin)
