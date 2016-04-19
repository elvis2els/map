#!/usr/bin/python
# -*- coding:utf-8 -*-
import argparse
import time
import os
import MySQLdb


parser = argparse.ArgumentParser(description="using to insert data to mysql")
parser.add_argument('path', help="input path, one day", nargs=1)
args = parser.parse_args()


def main():
    daypath = args.path[0]
    try:
        conn = MySQLdb.connect("localhost", "root", "369212hyl", "cabgps")
    except MySQLdb.Error as e:
        print "Connect Error: %s" % e
    cursor = conn.cursor()

    for root, days, filenames in os.walk(daypath):
        for day in days:
            begin = time.time()
            selectSQL = 'select cabid from d%s group by cabid' % day
            try:
                cursor.execute(selectSQL)
                results = cursor.fetchall()
                ofs = open('/dataPool/map/inputfile/d%s_cabid.txt' % day, 'w')
                lines = []
                for line in results:
                    lines.append(line[0] + '\n')
                ofs.writelines(lines)
                ofs.close()
            except Exception as e:
                print e
                return
            print 'success: %s' % day, time.time() - begin

if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
