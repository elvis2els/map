# -*- coding: utf-8 -*-
# 计算各时段mfp的平均通行时间
import time
from concurrent import futures

import pymysql
import pandas as pd
import numpy as np

from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


def get_edge_meta_ids(start_meta_id=None, end_meta_id=None):
    query = "SELECT DISTINCT edge_meta_id FROM main_path_odgroup"
    if start_meta_id is not None:
        query += " WHERE edge_meta_id >= {}".format(start_meta_id)
    if end_meta_id is not None:
        if start_meta_id is None:
            query += " WHERE "
        else:
            query += " AND "
        query += "edge_meta_id <= {}".format(end_meta_id)
    connection = pool.connection()
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        edge_meta_ids = cursor.fetchall()
        return [meta_id[0] for meta_id in edge_meta_ids]
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()


def get_main_path(edge_meta_id):
    query = "SELECT start_group_time, end_group_time, path, weekday FROM main_path_odgroup WHERE edge_meta_id={}".format(
        edge_meta_id)
    connection = pool.connection()
    try:
        return pd.read_sql_query(query, connection)
    except Exception as e:
        print(e)
    finally:
        connection.close()


def mfp_path_unique(mfp_detail_df):
    # 由于数据库中mfp_path路口号间有空格,要特殊处理
    mfp_path_uni = mfp_detail_df['path'].drop_duplicates().tolist()
    mfp_path_uni2 = []
    for i in range(len(mfp_path_uni)):
        mfp_path = mfp_path_uni[i][1:-1].split(',')
        mfp_path_uni2.append(', '.join(mfp_path))
    return mfp_path_uni, mfp_path_uni2


def single_mfp_cost(mfp_path):
    """获取mfp_path每个时间段的平均通行时间"""
    mfp_path_list = mfp_path.split(", ")
    start_cross, end_cross = mfp_path_list[0], mfp_path_list[-1]
    query = """
SELECT
  unix_timestamp(end_time) - unix_timestamp(start_time) AS cost,
  (hour(start_time) * 60 + minute(start_time)) DIV 15   AS time_group,
  CASE WHEN weekday(start_time) < 5
    THEN 1
  ELSE 0 END                                            AS weekday
FROM (
       SELECT
         b0.metadata_id,
         start_time,
         time AS end_time
       FROM (
              SELECT
                metadata_id,
                time AS start_time
              FROM (
                     SELECT id
                     FROM traj_metadata
                     WHERE path LIKE '%{mfp}%') AS a0
                JOIN (SELECT
                        traj1.metadata_id,
                        time
                      FROM (
                             SELECT
                               metadata_id,
                               time
                             FROM traj_data
                             WHERE cross_id = {start_cross}) AS traj1
                        JOIN (SELECT metadata_id
                              FROM traj_data
                              WHERE cross_id = {start_cross}
                              GROUP BY metadata_id
                              HAVING count(metadata_id) = 1) AS traj2 ON traj1.metadata_id = traj2.metadata_id
                     ) AS a1 ON a0.id = a1.metadata_id) AS b0
         JOIN (SELECT
                 traj3.metadata_id,
                 time
               FROM (
                      SELECT
                        metadata_id,
                        time
                      FROM traj_data
                      WHERE cross_id = {end_cross}) AS traj3
                 JOIN (SELECT metadata_id
                       FROM traj_data
                       WHERE cross_id = {end_cross}
                       GROUP BY metadata_id
                       HAVING count(metadata_id) = 1) AS traj4 ON traj3.metadata_id = traj4.metadata_id) AS b1
           ON b0.metadata_id = b1.metadata_id) AS c""".format(mfp=mfp_path, start_cross=start_cross,
                                                              end_cross=end_cross)

    connection = pool.connection()
    try:
        mfp_cost_df = pd.read_sql_query(query, connection)
        mfp_cost_weekday = mfp_cost_df[mfp_cost_df['weekday'] == 1]
        mfp_cost_weekday = filter_outlier_cost(mfp_cost_weekday)
        mfp_cost_weekend = mfp_cost_df[mfp_cost_df['weekday'] == 0]
        mfp_cost_weekend = filter_outlier_cost(mfp_cost_weekend)
        mfp_cost_weekday, mfp_cost_weekend = mfp_avg_cost(mfp_cost_weekday, mfp_cost_weekend)

        return filter_cost(mfp_cost_weekday).append(filter_cost(mfp_cost_weekend)).set_index('time_group', drop=True)
    except Exception as e:
        print(e)
    finally:
        connection.close()


def filter_outlier_cost(mfp_cost_df):
    """用箱线图过滤"""
    grouped = mfp_cost_df.groupby('time_group')
    mfp_cost_df['lower'] = grouped['cost'].transform(
        lambda x: x.quantile(q=.25) - (1.5 * (x.quantile(q=.75) - x.quantile(q=.25))))
    mfp_cost_df['upper'] = grouped['cost'].transform(
        lambda x: x.quantile(q=.25) + (1.5 * (x.quantile(q=.75) - x.quantile(q=.25))))
    mfp_cost_df = mfp_cost_df[(mfp_cost_df['cost'] >= mfp_cost_df['lower']) & (mfp_cost_df['cost'] <= mfp_cost_df['upper'])]
    del mfp_cost_df['lower']
    del mfp_cost_df['upper']
    return mfp_cost_df


def mfp_avg_cost(mfp_cost_weekday, mfp_cost_weekend):
    grouped = mfp_cost_weekday.groupby('time_group', as_index=False)
    weekday_df = grouped['cost'].aggregate(np.mean)
    weekday_df['weekday'] = 1

    grouped = mfp_cost_weekend.groupby('time_group', as_index=False)
    weekend_df = grouped['cost'].aggregate(np.mean)
    weekend_df['weekday'] = 0
    return weekday_df, weekend_df


# def single_mfp_cost(mfp_path):
#     """获取mfp_path每个时间段的平均通行时间"""
#     mfp_path_list = mfp_path.split(", ")
#     start_cross, end_cross = mfp_path_list[0], mfp_path_list[-1]
#     query = """
# SELECT
#   avg(cost) AS avg_cost,
#   time_group,
#   weekday
# FROM (
#        SELECT
#          unix_timestamp(end_time) - unix_timestamp(start_time) AS cost,
#          (hour(start_time) * 60 + minute(start_time)) DIV 15   AS time_group,
#          CASE WHEN weekday(start_time) < 5
#            THEN 1
#          ELSE 0 END                                            AS weekday
#        FROM (
#               SELECT
#                 b0.metadata_id,
#                 start_time,
#                 time AS end_time
#               FROM (
#                      SELECT
#                        metadata_id,
#                        time AS start_time
#                      FROM (
#                             SELECT id
#                             FROM traj_metadata
#                             WHERE path LIKE '%{mfp}%') AS a0
#                        JOIN (SELECT
#                                traj1.metadata_id,
#                                time
#                              FROM (
#                                     SELECT
#                                       metadata_id,
#                                       time
#                                     FROM traj_data
#                                     WHERE cross_id = {start_cross}) AS traj1
#                                JOIN (SELECT metadata_id
#                                      FROM traj_data
#                                      WHERE cross_id = {start_cross}
#                                      GROUP BY metadata_id
#                                      HAVING count(metadata_id) = 1) AS traj2 ON traj1.metadata_id = traj2.metadata_id
#                             ) AS a1 ON a0.id = a1.metadata_id) AS b0
#                 JOIN (SELECT
#                         traj3.metadata_id,
#                         time
#                       FROM (
#                              SELECT
#                                metadata_id,
#                                time
#                              FROM traj_data
#                              WHERE cross_id = {end_cross}) AS traj3
#                         JOIN (SELECT metadata_id
#                               FROM traj_data
#                               WHERE cross_id = {end_cross}
#                               GROUP BY metadata_id
#                               HAVING count(metadata_id) = 1) AS traj4 ON traj3.metadata_id = traj4.metadata_id) AS b1
#                   ON b0.metadata_id = b1.metadata_id) AS c) AS d
# GROUP BY time_group, weekday""".format(mfp=mfp_path, start_cross=start_cross, end_cross=end_cross)
#
#     connection = pool.connection()
#     cursor = connection.cursor()
#     try:
#         mfp_cost_df = pd.read_sql_query(query, connection)
#         mfp_cost_weekday = mfp_cost_df[mfp_cost_df['weekday'] == 1].sort_values(by=['time_group'])
#         mfp_cost_weekend = mfp_cost_df[mfp_cost_df['weekday'] == 0].sort_values(by=['time_group'])
#         return filter_cost(mfp_cost_weekday).append(filter_cost(mfp_cost_weekend)).set_index('time_group', drop=True)
#     except Exception as e:
#         print(e)
#     finally:
#         cursor.close()
#         connection.close()


def filter_cost(mfp_cost_df):
    """修正异常值,cost不超过前两个时段之和,不小于前两个时段之差,若为异常值,则用前两个时段平均值修正"""
    if len(mfp_cost_df) <= 2:
        return mfp_cost_df
    mfp_cost_df.reset_index(drop=True, inplace=True)
    pre_1, pre_2 = mfp_cost_df.loc[1, 'cost'], mfp_cost_df.loc[0, 'cost']  # 前第一个和前第二个的平均cost
    for i in range(2, len(mfp_cost_df)):
        avg = (pre_1 + pre_2) / 2
        if not avg / 2.5 < mfp_cost_df.loc[i, 'cost'] < avg * 2.5:
            mfp_cost_df.loc[i, 'cost'] = (pre_1 + pre_2) / 2
        pre_2, pre_1 = pre_1, mfp_cost_df.loc[i, 'cost']
    return mfp_cost_df


def group_time_cost(mfp_path, mfp_cost_detail, mfp_detail_df, edge_id):
    """获取每个时间段的cost"""
    mfp_time_range = mfp_detail_df[mfp_detail_df['path'] == mfp_path][['start_group_time', 'end_group_time', 'weekday']]
    mfp_cost_ret = []
    for index, time_range in mfp_time_range.iterrows():
        start_time, end_time, weekday = time_range
        mfp_cost_df = mfp_cost_detail[mfp_cost_detail['weekday'] == weekday]
        if len(mfp_cost_df) == 0:  # 若不存在直接到达的路段,则该mfp路径为拼接出来的.暂时以0填充，之后再修正
            for time_group in range(start_time, end_time + 1):
                mfp_cost_ret.append([edge_id, time_group, 0, int(weekday)])
            continue
        index_set = set(mfp_cost_df.index)
        min_index, max_index = min(index_set), max(index_set)
        for time_group in range(start_time, end_time + 1):
            if time_group in index_set:
                cost = mfp_cost_df['cost'][time_group]
            else:
                # 若无法获取到time_group,则从该时段前后求平均值获取
                if min_index >= time_group:
                    pre_time_group = min_index
                else:
                    pre_time_group = time_group - 1
                    while pre_time_group not in index_set:
                        pre_time_group -= 1
                if max_index <= time_group:
                    post_time_group = max_index
                else:
                    post_time_group = time_group + 1
                    while post_time_group not in index_set:
                        post_time_group += 1
                cost = (mfp_cost_df['cost'][pre_time_group] + mfp_cost_df['cost'][post_time_group]) / 2
            mfp_cost_ret.append([edge_id, time_group, float(cost), int(weekday)])
    return mfp_cost_ret


def to_sql(mfp_cost_group):
    query = "INSERT INTO visual_edge_cost_filter (edge_meta_id, time_group, cost, weekday) values(%s, %s, %s, %s)"
    connection = pool.connection()
    cursor = connection.cursor()
    try:
        cursor.executemany(query, mfp_cost_group)
        connection.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        connection.close()


def mfp_cost(edge_meta_id):
    mfp_detail_df = get_main_path(edge_meta_id)
    origin_mfp_paths, mfp_paths = mfp_path_unique(mfp_detail_df)
    mfp_cost_group = []
    for i in range(len(origin_mfp_paths)):
        mfp_cost_detail = single_mfp_cost(mfp_paths[i])
        mfp_cost_group.extend(group_time_cost(origin_mfp_paths[i], mfp_cost_detail, mfp_detail_df, edge_meta_id))
    return mfp_cost_group


def main():
    edge_meta_ids = get_edge_meta_ids()[:]
    i, max_len = 0, len(edge_meta_ids)
    s_time = time.time()
    print('begin')
    for mfp_cost_group in map(mfp_cost, edge_meta_ids):
        to_sql(mfp_cost_group)
        print('{}/{} done using time {}'.format(i, max_len, time.time() - s_time))
        i += 1


if __name__ == '__main__':
    time_s = time.time()
    main()
    print('using time: {}'.format(time.time() - time_s))
