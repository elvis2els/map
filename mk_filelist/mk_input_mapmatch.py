#!/usr/bin/python3
# -*- coding:utf-8 -*-
import os
import argparse
import time

parser = argparse.ArgumentParser(
    description="using to make mapmach input and ouput path file")
parser.add_argument(
    'path', help="input source path, ouput destination path and the inoutput file path", nargs=3)
parser.add_argument(
    'range', help="the index of cabids, from and to", nargs=2, type=int)
args = parser.parse_args()


def main():
    srcpath = args.path[0]
    despath = args.path[1]
    inoutputfile = args.path[2]
    if not os.path.exists(srcpath):
        raise Exception("srcpath is not exists")
    inoutputPath = []
    cabids = os.listdir(srcpath)
    pos = args.range[0]
    maxpos = len(cabids)
    while pos < args.range[1] and pos < maxpos:
        id = cabids[pos]
        cabidpath = os.path.join(srcpath, id)
        days = os.listdir(cabidpath)
        for day in days:
            for cabidfile in os.listdir(os.path.join(cabidpath, day)):
                inoutputPath.append(os.path.join(
                    cabidpath, day, cabidfile) + '\n')
                inoutputPath.append(os.path.join(
                    despath , id, day, cabidfile) + '\n')
        pos += 1
    f = open(inoutputfile, 'w')
    f.writelines(inoutputPath)
    f.close()


if __name__ == '__main__':
    begin = time.time()
    if args.path:
        main()
    end = time.time()
    print(end - begin)
