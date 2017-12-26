# -*- coding: utf-8 -*-
import os
import time
from concurrent import futures
from datetime import timedelta

import pymysql

from Config import Config
from path_restore import MainRoad
from roadmap import Roadmap
from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)

config = Config()
road = Roadmap(config.getConf('analizeTime')['map_file'])
road.load()


def get_visual_edge():
    database_conf = config.getConf('database')
    connection = pymysql.connect(
        database_conf['host'],
        database_conf['user'],
        database_conf['passwd'],
        database_conf['name'])
    with connection.cursor() as cursor:
        query = 'SELECT id, start_cross_id, end_cross_id FROM visual_edge_odgroup'
        cursor.execute(query)
        edges = cursor.fetchall()
        return edges


def getMainRoad(edge):
    pid = os.getpid()
    print('start process: {}'.format(pid))
    mainRoad = MainRoad(road)
    meta_id, start_id, end_id = edge
    print('process {0} weedday begin!'.format(pid))
    mainPath_weekday = mainRoad.getMainPath(start_id, end_id)
    print('process {0} weedday done!'.format(pid))
    mainPath_weekend = mainRoad.getMainPath(start_id, end_id, weekday=False)
    print('process {0} weedend done!'.format(pid))
    return (meta_id, mainPath_weekday, mainPath_weekend)


def toValues(mfps, meta_id, weekday=True):
    if weekday:
        weekday = 1
    else:
        weekday = 0
    values = []
    for mfp in mfps:
        values.append([int(meta_id), int(mfp[0][0]), int(
            mfp[0][1]), ','.join(map(str, mfp[1])), weekday])
    return values


def toSql(query, values):
    connection = pool.connection()
    cursor = connection.cursor()
    cursor.executemany(query, values)
    connection.commit()
    cursor.close()
    connection.close()


def main():
    count = 2062
    edges = get_visual_edge()[count:4000]
    #print(edges[0]);exit()
    values, i, maxlen = [], count, len(edges) + count
    query = 'INSERT INTO main_path_odgroup (edge_meta_id, start_group_time, end_group_time, path, weekday) values(%s, %s, %s, "%s", %s);'
    time_s = time.time()
    with futures.ProcessPoolExecutor(max_workers=4) as pool:
        for meta_id, mfp_weekday, mfp_weekend in pool.map(getMainRoad, edges):
            values.extend(toValues(mfp_weekday, meta_id))
            values.extend(toValues(mfp_weekend, meta_id, weekday=False))
            i += 1
            print('toSql')
            toSql(query, values)
            print('toSql done')
            values = []
            print(u'{}/{}, using time: {}'.format(i, maxlen, timedelta(seconds=time.time() - time_s)))
    # for meta_id, mfp_weekday, mfp_weekend in map(getMainRoad, edges):
    #     values.extend(toValues(mfp_weekday, meta_id))
    #     values.extend(toValues(mfp_weekend, meta_id, weekday=False))
    #     i += 1
    #     toSql(query, values)
    #     values = []
    #     print('{}/{}, 使用时间: {}'.format(i, maxlen, timedelta(seconds=time.time() - time_s)));exit()

if __name__ == '__main__':
    time_s = time.time()
    main()
    # getMainRoad(get_visual_edge()[1406])
    print('using time: {}'.format(time.time() - time_s))
