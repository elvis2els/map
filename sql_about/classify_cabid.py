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


def main(filename, j):
    path = '/dataPool/map/2015cabGPS_splitpath/'
    day = filename[1:9]
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
        begin = time.time()
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
        print '[%d][%d]Success %s: %.2f.................%.2f%%  sum time: %s' % (j, i, cabid, time.time() - begin, float(i) / max * 100, time.time() - start)
        i += 1
    conn.close()

start = time.time()
if __name__ == '__main__':
    days = os.listdir('/dataPool/map/inputfile/')
    j = 1
    for day in days:
        main(day, j)
        j += 1
    print time.time() - start
