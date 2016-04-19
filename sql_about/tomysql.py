#!/usr/bin/python
# -*- coding: UTF-8 -*-
import argparse
import time
import os
import MySQLdb

parser = argparse.ArgumentParser(description="using to insert data to mysql")
parser.add_argument('path', help="input path, one day", nargs=1)
args = parser.parse_args()


def main(filepath):
    path = filepath
    path = path.strip('\n') 
    try:
        conn = MySQLdb.connect("localhost", "root", "369212hyl", "cabgps")
    except MySQLdb.Error as e:
        print "Connect Error：%s" % e
    cursor = conn.cursor()
    daypath = time.strptime(path[-9:-1], '%Y%m%d')
    insertSQL = 'insert into d%s (time, x, y, direction, speed, status, cabid)' % path[-9:-1]
    insertSQL = insertSQL + 'values(%s,%s,%s,%s,%s,%s,%s);'
    i = 1;
    for root, dirnames, filenames in os.walk(path):
        for file in filenames:
            start = time.time();
            print 'Reading file： %s' % (path + file)
            f = open(path + file, 'r')
            lines = f.readlines()
            Values = []
            j = 1
            for line in lines:
                try:
                    t = time.strptime(line[:19], '%Y-%m-%d %H:%M:%S')
                except ValueError as e:
                    print 'time.strptime error： line[%d]' % j
                    print e;exit()
                j += 1
                if t[0] == daypath[0] and t[1] == daypath[1] and t[2] == daypath[2]: 
                    data = line.split(',')
                    if data[5] == '10000000':
                        data[5] = '1'
                    else:
                        data[5] = '0'   
                    data[6] = data[6].strip('\n')
                    Values.append(data)
            cursor.executemany(insertSQL,Values)
            conn.commit()
            print "[%d]finished: %s, use time： %s" % (i, file, time.time()-start)
            i += 1


if __name__ == '__main__':
    start = time.time()
    f = open(args.path[0],'r')
    filepaths = f.readlines()
    for filepath in filepaths:
        main(filepath)
    print time.time() - start
