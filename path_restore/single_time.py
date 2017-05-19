#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import time
import os

from path_restore import EstTime

parser = argparse.ArgumentParser(description="查找两地标间主路段，并计算通行时间")
parser.add_argument('start', help='起始路口')
parser.add_argument('end', help='终点路口')
parser.add_argument('weekday', choices=['weekday', 'weekend'], help='工作日or周末')
args = parser.parse_args()

def main():
    estTime = EstTime()
    estTime_df = estTime.getEstTime(args.start, args.end, args.weekday)
    del estTime

if __name__ == '__main__':
    start_time = time.time()
    main()
    print('用时:{}'.format(time.time() - start_time))