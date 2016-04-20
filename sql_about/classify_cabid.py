#!/usr/bin/python
#-*-coding:UTF-8-*-
import argparse
import os
import time
import MySQLdb
import sys


def createResult(results):
    lines = []
    dot = ','
    for line in results:
        line = list(line)
        del line[0]
        del line[6]
        line[0] = line[0].strftime('%Y-%m-%d %H:%M:%S')
        line[1], line[2] = str(line[1]), str(line[2])
        lines.append(dot.join(line) + '\n')
    return lines


def main():
    path = '/dataPool/map/2015cabGPS_splitpath/'
    day = '20151030'
    try:
        conn = MySQLdb.connect("127.0.0.1", "root", "369212hyl", "cabgps")
    except:
        print 'Connect MySQL Error'
        sys.exit()
    cursor = conn.cursor()
    ifs = open('/dataPool/map/inputfile/d%s_cabid.txt' % day, 'r')
    cabids = ifs.readlines()
    ifs.close()
    max = len(cabids)
    i = 1
    for cabid in cabids:
        cabid = cabid.strip('\n')
        start = time.time()
        selectSQL = 'select * from d%s where cabid=\'%s\' order by time;' % (
            day, cabid)
        try:
            cursor.execute(selectSQL)
            results = cursor.fetchall()
        except Exception as e:
            print e
        results = createResult(results)
        ofs = open(path + cabid + '/' + day + '.txt', 'w')
        ofs.writelines(results)
        ofs.close()
        print '[%d]Success %s: %s................%.2f\%' % (i, cabid, time.time() - start, float(i) / max * 100)
        i += 1


if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
