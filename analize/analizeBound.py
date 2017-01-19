#!/usr/bin/python3
# -*- coding:utf-8 -*-
# 分析待匹配路段中是否有超过最新地图数据范围
import os
import argparse
import time

parser = argparse.ArgumentParser(
    description="using to ")
parser.add_argument(
    'path', help="input path and output path", nargs=1)
args = parser.parse_args()


def main():
    path = args.path[0]
    ifs = open(path, 'r')
    count = 0
    filepath = ifs.readline()
    while len(filepath) != 0:
        cabfile = open(filepath[:-1], 'r')
        lines = cabfile.readlines()
        for line in lines:
            colunms = line.split(',')
            if not (float(colunms[1]) > 12922000 and float(colunms[1]) < 12998676 and float(colunms[2]) > 4819554 and float(colunms[2]) < 4892119):
                count += 1
                print("Path: " + filepath)
        ifs.readline()
        filepath = ifs.readline()      
    print(count)


if __name__ == '__main__':
    begin = time.time()
    if args.path:
        main()
    end = time.time()
    print(end - begin)
