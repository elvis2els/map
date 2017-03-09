#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import argparse
import traceback
import time

import pymysql

parser = argparse.ArgumentParser(description="获取两个路口间行驶时间")
parser.add_argument('start', help="起始路口号")
parser.add_argument('end', help="终止路口号")
args = parser.parse_args()


def printRepet(rows):
    pre = 0
    for row in rows:
        if row[0] == pre:
            print(row)
        else:
            pre = row[0]


def main():
    db = pymysql.connect('localhost', 'root', '369212', 'path_restore')
    cursor = db.cursor()

    startQuery = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
        args.start)
    endQuery = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
        args.end)

    try:
        start_time = time.time()
        cursor.execute(startQuery)
        start_result = cursor.fetchall()
        start_ids = [start_id[0] for start_id in start_result]
        print("start_ids用时：{}".format(time.time() - start_time))
        print("start_id长度:{}".format(len(start_ids)))

        start_time = time.time()
        cursor.execute(endQuery)
        end_result = cursor.fetchall()
        end_ids = [end_id[0] for end_id in end_result]
        print("end_ids用时：{}".format(time.time() - start_time))
        print("end_ids长度:{}".format(len(end_ids)))

    except:
        traceback.print_exc()
        exit()

    ids = set(start_ids) & set(end_ids)
    print("交集后长度:{}".format(len(ids)))

    start_rows = []
    for row in start_result:
        if row[0] in ids:
            start_rows.append(row)

    end_rows = []
    for row in end_result:
        if row[0] in ids:
            end_rows.append(row)

    print("重复的start")
    printRepet(start_rows)
    print("重复的end")
    printRepet(end_rows)

if __name__ == '__main__':
    main()
