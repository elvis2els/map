#!/usr/bin/python
# -*- coding: UTF-8 -*-
import argparse
import time
import os
import MySQLdb

parser = argparse.ArgumentParser(description="using to insert data to mysql")
parser.add_argument('path', help="root path", nargs=1)
args = parser.parse_args()


def main():
    try:
        conn = MySQLdb.connect("localhost", "root", "369212", "path_restore")
    except MySQLdb.Error as e:
        print "Connect Error：%s" % e
    cursor = conn.cursor()

    insertSQL = 'insert into traj_data (metadata_id, cross_id, time)'
    insertSQL = insertSQL + 'values(%s,%s,%s);'

    srcpath = args.path[0]
    if not os.path.exists(srcpath):
        raise Exception(srcpath + " is not exists")
    cabids = os.listdir(srcpath)   
    metadata_id = 1
    countcab = 1
    for cabid in cabids:
        scabtime = time.time()
        cabidpath = os.path.join(srcpath, cabid)
        days = os.listdir(cabidpath)
        values= []
        for day in days:
            for filename in os.listdir(os.path.join(cabidpath, day)):
                f = open(os.path.join(cabidpath, day, filename), 'r')
                lines = f.readlines()
                for line in lines:
                    meta = (str(metadata_id) + "," + line.strip('\n')).split(',')
                    values.append(meta)
                metadata_id += 1
        try:
            cursor.executemany(insertSQL, values)
            conn.commit()
            print "sucess: %d, using time: %s" % (countcab, time.time() - scabtime)
        except MySQLdb.Error as e:
            print "执行insert出错：%s" % e
        countcab += 1

    cursor.close()
    conn.close()


if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
