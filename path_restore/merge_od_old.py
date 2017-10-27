#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import time
import json

import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


def read_list_od():
    connection = pool.connection()
    cursor = connection.cursor()
    query = "SELECT max(od_group) FROM traj_metadata WHERE od_group is not null"
    cursor.execute(query)
    max_od_group = cursor.fetchone()[0]

    od_list = []
    query = "SELECT od_group, path FROM traj_metadata WHERE od_group>={} and od_group<={}".format(0, 11000)
    paths = pd.read_sql_query(query, connection)
    count_od = 0
    s_time = time.time()
    for od_group in range(0, 11000):
        count_od += 1
        new_od_list = paths[paths.od_group == od_group]
        if len(new_od_list) == 0:
            continue
        tmp_od_list = list(new_od_list.path)
        new_od_list = []
        for traj in tmp_od_list:
            new_od_list.append(json.loads(traj))
        od_list.append(new_od_list)
        if count_od % 100 == 0:
            print('{},{}'.format(od_group, time.time() - time_s))
        # 测试用
        if od_group == 10000:
            return od_list
    return od_list


def get_traj():
    connection = pool.connection()
    cursor = connection.cursor()
    query = "SELECT id, path FROM traj_metadata WHERE id>={} and id<={}".format(98157, 100000)
    cursor.execute(query)
    meta_datas = cursor.fetchall()
    cursor.close()
    connection.close()
    for metaid, path in meta_datas:
        yield metaid, json.loads(path)


def similarity(traj1, traj2):
    """两轨迹相似性计算"""
    straj1, straj2 = set(traj1), set(traj2)
    return float(len(straj1 & straj2)) / len(straj1 | straj2)


def is_same_group(traj, od_group):
    for od_traj in od_group:
        if similarity(traj, od_traj) < 0.8:
            return False
    return True


def analize_od():
    list_od = read_list_od()
    time_s = time.time()
    count = 0
    for metaid, traj_detail in get_traj():
        find_od_group = False
        count += 1
        for i, od_group in enumerate(list_od):
            if is_same_group(traj_detail, od_group):
                od_group.append(traj_detail)
                find_od_group = True
                break
        if not find_od_group:
            list_od.append([traj_detail])
        if count % 100 == 0:
            print('{},{}'.format(count, time.time() - time_s))
    print('{},{}'.format(count, time.time() - time_s))


if __name__ == '__main__':
    time_s = time.time()
    analize_od()
    print("total using time: {}".format(time.time() - time_s))
