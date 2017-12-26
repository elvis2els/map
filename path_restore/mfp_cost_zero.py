# -*- coding: utf-8 -*-
# mfp无法直接找到通行时间的,通过每一小段通行时间补全
import time
from concurrent import futures

import pymysql
import pandas as pd

from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


def get_cost0_details():
    query = "SELECT edge_meta_id, time_group, weekday FROM visual_edge_cost WHERE cost=0"
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        cost0_details = cursor.fetchall()
        return cost0_details
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def get_main_path(cost0_detail):
    edge_meta_id, time_group, weekday = cost0_detail
    query = """SELECT path
                FROM main_path_odgroup
                WHERE edge_meta_id = {edge_meta_id} AND weekday = {weekday} 
                      AND start_group_time <= {time_group} AND end_group_time >= {time_group}""".format(
        edge_meta_id=edge_meta_id, time_group=time_group, weekday=weekday)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        main_path = cursor.fetchone()[0][1:-1].split(',')
        return main_path
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def cross_cost(start_cross, end_cross, time_group, weekday):
    """获取相邻两路口间在指定时间内的通行时间"""
    query = """
    SELECT *
    FROM (
           SELECT
             unix_timestamp(end.time) - unix_timestamp(start.time) AS cost,
             (hour(start.time) * 60 + minute(start.time)) DIV 15   AS time_group,
             CASE WHEN weekday(start.time) < 5
               THEN 1
             ELSE 0 END                                            AS weekday
           FROM (
                  SELECT
                    id,
                    metadata_id,
                    time
                  FROM traj_data
                  WHERE cross_id = 57183) AS start
             JOIN (
                    SELECT
                      id,
                      metadata_id,
                      time
                    FROM traj_data
                    WHERE cross_id = 57036
                  ) AS end ON start.metadata_id = end.metadata_id
           WHERE end.id - start.id = 1) AS a WHERE time_group=67 AND weekday=1;
        """
    conn = pool.connection()
    try:
        cost_df = pd.read_sql_query(query, conn)
        cost_df = filter_outliers(cost_df)
        return cost_df['cost'].mean()
    except Exception as e:
        print(e)
    finally:
        conn.close()


def filter_outliers(df):
    q1, q3 = df['cost'].quantile(.25), df['cost'].quantile(.75)
    iqr = q3 - q1
    return df[(df['cost'] <= (q3 + 1.5 * iqr)) & (df['cost'] >= (q1 - 1.5 * iqr))]


def mfp_cost(main_path, time_group, weekday):
    cost = 0
    for start_cross, end_cross in zip(main_path[:-1], main_path[1:]):
        cost += cross_cost(start_cross, end_cross, time_group, weekday)
    return cost


def update_sql(cost0_detail, cost):
    edge_meta_id, time_group, weekday = cost0_detail
    update = """UPDATE visual_edge_cost SET cost={cost} 
        WHERE edge_meta_id={edge_meta_id} AND time_group={time_group} AND weekday={weekday}""".format(
        edge_meta_id=edge_meta_id, time_group=time_group, weekday=weekday, cost=cost)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(update)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        cursor.close()
        conn.close()


def main():
    cost0_details = get_cost0_details()
    i, maxlen = 1, len(cost0_details)
    for cost0_detail in cost0_details:
        main_path = get_main_path(cost0_detail)
        cost = mfp_cost(main_path, cost0_detail[1], cost0_detail[2])
        update_sql(cost0_detail, cost)
        print("{}/{} done".format(i, maxlen))
        i += 1



if __name__ == '__main__':
    time_s = time.time()
    main()
    print('using time: {}'.format(time.time() - time_s))
