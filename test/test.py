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

    ofs = open('/dataPool/map/inputfile/count_cabid', 'w')
    lines = []
    for root, days, filenames in os.walk(daypath):
        for day in days:
            begin = time.time()
            selectSQL = 'select count(*) from (select cabid from d%s group by cabid) as a' % day
            try:
                cursor.execute(selectSQL)
                results = cursor.fetchall()
                for line in results:
                    lines.append(day + ' ' + str(line[0]) + '\n')
            except Exception as e:
                print e
                return
    ofs.writelines(lines)
    ofs.close()

if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
