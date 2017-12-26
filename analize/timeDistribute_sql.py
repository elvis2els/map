#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import math

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


def draw_weekdays_plot(df):
    plt.plot(df['time_x'].dt.time, df['duration'], 'ob')
    plt.show()


def filter_outliers(df):
    q1, q3 = df['cost'].quantile(.25), df['cost'].quantile(.75)
    iqr = q3 - q1
    return df[(df['cost'] <= (q3 + 1.5 * iqr)) & (df['cost'] >= (q1 - 1.5 * iqr))]


def get_cross_time(start_cross, end_cross, weekday=1):
    query = """
SELECT 
  time,
  cost
FROM (
       SELECT
         time(start_time)                                      AS time,
         unix_timestamp(end_time) - unix_timestamp(start_time) AS cost,
         CASE WHEN weekday(start_time) < 5
           THEN 1
         ELSE 0 END                                            AS weekday
       FROM (
              SELECT
                start.id   AS start_id,
                end.id     AS end_id,
                start.metadata_id,
                start.time AS start_time,
                end.time   AS end_time
              FROM (
                     SELECT *
                     FROM traj_data
                     WHERE cross_id = {start_cross}) AS start
                JOIN (
                       SELECT *
                       FROM traj_data
                       WHERE cross_id = {end_cross}
                     ) AS end ON start.metadata_id = end.metadata_id
              WHERE end.id > start.id
            ) AS a
         JOIN (SELECT metadata_id
               FROM (
                      SELECT
                        start.id   AS start_id,
                        end.id     AS end_id,
                        start.metadata_id,
                        start.time AS start_time,
                        end.time   AS end_time
                      FROM (
                             SELECT *
                             FROM traj_data
                             WHERE cross_id = {start_cross}) AS start
                        JOIN (
                               SELECT *
                               FROM traj_data
                               WHERE cross_id = {end_cross}
                             ) AS end ON start.metadata_id = end.metadata_id
                      WHERE end.id > start.id
                    ) AS c
               GROUP BY c.metadata_id
               HAVING count(*) = 1) AS b ON a.metadata_id = b.metadata_id) AS d
WHERE weekday = {weekday}""".format(start_cross=start_cross, end_cross=end_cross, weekday=weekday)
    conn = pool.connection()
    try:
        cost_df = pd.read_sql_query(query, conn)
        cost_df['time'] = cost_df['time'] / np.timedelta64(1, 'h')
        return cost_df
    except Exception as e:
        print(e)
    finally:
        conn.close()


def get_mfp_time(start_cross, end_cross, weekday=1):
    query = """
SELECT time_group, cost
FROM visual_edge_cost_filter
WHERE edge_meta_id = (
  SELECT id
  FROM visual_edge_odgroup
  WHERE start_cross_id = {start_cross} AND end_cross_id = {end_cross}) AND weekday={weekday}""".format(
        start_cross=start_cross, end_cross=end_cross, weekday=weekday)
    conn = pool.connection()
    try:
        mfp_cost_df = pd.read_sql_query(query, conn)
        mfp_cost_df['time_group'] = mfp_cost_df['time_group'] * 15 / 60
        return mfp_cost_df.sort_values(by=['time_group'])
    except Exception as e:
        print(e)
    finally:
        conn.close()


def main():
    start_cross, end_cross, weekday = 52528, 57213, 1
    cost_df = get_cross_time(start_cross, end_cross, weekday)
    # cost_df = filter_outliers(cost_df)
    mfp_cost_df = get_mfp_time(start_cross, end_cross, weekday)
    plt.plot(cost_df['time'], cost_df['cost'], '.g', mfp_cost_df['time_group'], mfp_cost_df['cost'], 'r-',
             drawstyle='steps-post')
    plt.show()


if __name__ == '__main__':
    main()
