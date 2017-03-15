#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import argparse
import datetime
import time
import traceback

import numpy as np
import pandas as pd
import pymysql

parser = argparse.ArgumentParser(description="获取两个路口间行驶时间")
parser.add_argument('start', help="起始路口号")
parser.add_argument('end', help="终止路口号")
args = parser.parse_args()


def getCrossRows(connection, crossId):
    Query = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
        crossId)
    with connection.cursor() as cursor:
        cursor.execute(Query)
        result = cursor.fetchall()
    return result


def main():
    connection = pymysql.connect('localhost', 'root', '369212', 'path_restore')

    startQuery = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
        args.start)
    endQuery = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
        args.end)

    try:
        start_time = time.time()
        start_result = getCrossRows(connection, args.start)
        start_ids = [start_id[0] for start_id in start_result]
        print("start_ids用时：{}".format(time.time() - start_time))
        print("start_id长度:{}".format(len(start_ids)))

        start_time = time.time()
        end_result = getCrossRows(connection, args.end)
        end_ids = [end_id[0] for end_id in end_result]
        print("end_ids用时：{}".format(time.time() - start_time))
        print("end_ids长度:{}".format(len(end_ids)))

    except:
        traceback.print_exc()
        exit()
    finally:
        connection.close()

    start_df = pd.DataFrame(list(start_result), columns=['meta_id', 'time'])
    end_df = pd.DataFrame(list(end_result), columns=['meta_id', 'time'])

    merge_df = pd.merge(start_df, end_df, on='meta_id')
    print(merge_df)
    merge_df['counts'] = merge_df.groupby(
        ['meta_id'])['meta_id'].transform(len)
    # 删除出现两次以上和start_time<end_time的数据
    merge_df = merge_df[merge_df.counts == 1 &
                        (merge_df.time_x < merge_df.time_y)]
    del merge_df['counts']

    merge_df['duration'] = merge_df.time_y - merge_df.time_x
    print(merge_df)

    merge_df.to_csv('test.csv')

if __name__ == '__main__':
    main()
