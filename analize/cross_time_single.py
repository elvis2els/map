#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import argparse
import datetime
import os
import time
import traceback

import numpy as np
import pandas as pd
import pymysql

parser = argparse.ArgumentParser(description="获取两个路口间行驶时间")
parser.add_argument('start', help="起始路口号")
parser.add_argument('end', help="终止路口号")
parser.add_argument('-o', '--output', default='~/map/analize/analizeTime/countXEntTime/',
                    help='输出文件路径,文件为{output}/{start}to{end}/{start}to{end}.csv,默认output为~/map/analize/analizeTime/countXEntTime/')
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

    try:
        start_time = time.time()
        start_result = getCrossRows(connection, args.start)
        print("start_ids用时：{}".format(time.time() - start_time))
        print("start_id长度:{}".format(len(start_result)))

        start_time = time.time()
        end_result = getCrossRows(connection, args.end)
        print("end_ids用时：{}".format(time.time() - start_time))
        print("end_ids长度:{}".format(len(end_result)))

    except:
        traceback.print_exc()
        exit()
    finally:
        connection.close()

    start_df = pd.DataFrame(list(start_result), columns=['meta_id', 'time'])
    end_df = pd.DataFrame(list(end_result), columns=['meta_id', 'time'])

    merge_df = pd.merge(start_df, end_df, on='meta_id')
    merge_df['counts'] = merge_df.groupby(
        ['meta_id'])['meta_id'].transform(len)
    # 删除出现两次以上和start_time>end_time的数据
    merge_df = merge_df[merge_df.counts == 1 &
                        (merge_df.time_x < merge_df.time_y)]
    del merge_df['counts']

    merge_df['duration'] = merge_df.time_y - merge_df.time_x

    out_dir = os.path.join(
        args.output, '{}to{}'.format(args.start, args.end))
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    merge_df.to_csv(os.path.join(out_dir, '{}to{}.csv'.format(
        args.start, args.end)), index=False)

    print("最终长度：{}".format(len(merge_df)))

if __name__ == '__main__':
    main()
