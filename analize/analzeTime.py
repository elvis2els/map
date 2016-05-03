#!/usr/bin/python3
# -*- coding:utf-8 -*-
import time
import os
import sys

path = '/dataPool/map/2015cabGPS_splitpath/'
outputPath = '/dataPool/map/analizeTime/'


def countTime(lines, count):
    pretime = time.mktime(time.strptime(lines[0][0:19], '%Y-%m-%d %H:%M:%S'))
    posttime = time.mktime(time.strptime(lines[1][0:19], '%Y-%m-%d %H:%M:%S'))
    chtime = posttime - pretime
    if chtime not in count:
        count[chtime] = 1
    else:
        count[posttime - pretime] += 1
    i = 2
    while i < len(lines):
        pretime = posttime
        posttime = time.mktime(time.strptime(
            lines[i][0:19], '%Y-%m-%d %H:%M:%S'))
        chtime = posttime - pretime
        if chtime not in count:
            count[chtime] = 1
        else:
            count[posttime - pretime] += 1
        i += 1


def writeLines(day):
    dirnames = os.listdir(path)
    count = {}
    begin = time.time()
    i = 1
    for cabid in dirnames:
        try:
            ifs = open(path + cabid + '/' + day + '.txt', 'r')
        except Exception as e:
            continue
        lines = ifs.readlines()
        ifs.close()
        if len(lines) < 10:
            efs = open(outputPath + 'error.log', 'a')
            efs.write('cabid: {0}, day: {1}\n'.format(cabid, day))
            efs.close()
            continue
        countTime(lines, count)
        if i % 100 == 0:
            print('day: {1}, finish {0}'.format(i, day))
        i += 1
    ofs = open(outputPath + day + '.txt', 'w')
    outlines = []
    for chtime, times in count.items():
        outlines.append('{0},{1}\n'.format(chtime, times))
    ofs.writelines(outlines)
    ofs.close()
    print('finish: {0}, using time: {1}'.format(day, time.time() - begin))


def main():
    days = ['20151030', '20151031']
    i = 20151101
    while i <= 20151130:
        days.append(str(i))
        i += 1
    for day in days:
        writeLines(day)


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
