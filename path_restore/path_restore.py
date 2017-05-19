#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import math
import os
import datetime as dt

import numpy as np
import pandas as pd
import pymysql

from Config import Config


class EstTime(object):
    """分析两点间通行时间"""

    def __init__(self):
        self.config = Config()
        database_conf = self.config.getConf('database')
        self.connection = pymysql.connect(
            database_conf['host'],
            database_conf['user'],
            database_conf['passwd'],
            database_conf['name']
        )

    def est_cross_time(self, start_id, end_id):
        """估计两点通行时间"""
        def traj_pass_cross(cross_id):
            """获取通过该cross的轨迹id"""
            query = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
                cross_id)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                traj_ids = cursor.fetchall()
            return traj_ids

        def merged_trajId(start_id, end_id):
            """合并后且无重复、s.time<e.time"""
            start_ids = traj_pass_cross(start_id)
            end_ids = traj_pass_cross(end_id)

            start_df = pd.DataFrame(
                list(start_ids), columns=['meta_id', 'time'])
            end_df = pd.DataFrame(list(end_ids), columns=['meta_id', 'time'])

            merge_df = pd.merge(start_df, end_df, on='meta_id')
            merge_df['counts'] = merge_df.groupby(
                ['meta_id'])['meta_id'].transform(len)
            merge_df = merge_df[merge_df.counts == 1 &
                                (merge_df.time_x < merge_df.time_y)]
            merge_df = merge_df.drop('counts', axis=1)
            merge_df['duration'] = (
                merge_df.time_y - merge_df.time_x) / np.timedelta64(1, 's')
            merge_df['time_group'] = ((merge_df['time_x'].dt.hour * 60 + merge_df['time_x'].dt.minute) /
                                      int(self.config.getConf('analizeTime')['time_interval'])).apply(math.ceil)
            return merge_df

        def is_outlier(row, state):
            q1 = state.loc[row.time_group]['q1']
            q3 = state.loc[row.time_group]['q3']
            iqr = q3 - q1
            if row.duration > (q3 + 1.5 * iqr) or row.duration < (q1 - 1.5 * iqr):
                return True
            else:
                return False

        def filter_outliers(df):
            grouped = df.groupby('time_group')
            stat_before = pd.DataFrame(
                {'q1': grouped['duration'].quantile(.25), 'q3': grouped['duration'].quantile(.75)})
            df['outlier'] = df.apply(is_outlier, axis=1, args=(stat_before,))
            df = df[~(df.outlier)]
            df['weekday'] = df['time_x'].dt.weekday
            return df.drop('outlier', axis=1)

        def to_csv(df, start_id, end_id):
            filebase = '{}to{}'.format(start_id, end_id)
            dirpath = os.path.join(self.config.getConf(
                'analizeTime')['homepath'], filebase)
            if not os.path.exists(dirpath):
                os.mkdir(dirpath)
            df.to_csv(os.path.join(
                dirpath, '{}.csv'.format(filebase)), index=False)
            df_weekday = df[df['weekday'] < 6]
            df_weekend = df[df['weekday'] >= 6]

            df_weekday.to_csv(os.path.join(
                dirpath, 'weekday.csv'), index=False)
            df_weekend.to_csv(os.path.join(
                dirpath, 'weekend.csv'), index=False)

            grouped_weekday = df_weekday['duration'].groupby(df['time_group'])
            grouped_weekday.mean().to_csv(os.path.join(dirpath, 'weekday-time.csv'))
            grouped_weekend = df_weekend['duration'].groupby(df['time_group'])
            grouped_weekend.mean().to_csv(os.path.join(dirpath, 'weekend-time.csv'))

        filebase = '{}to{}'.format(start_id, end_id)
        dirpath = os.path.join(self.config.getConf(
            'analizeTime')['homepath'], filebase)
        filepath = os.path.join(dirpath, '{}.csv'.format(filebase))
        if not os.path.exists(filepath):
            merge_df = merged_trajId(start_id, end_id)
            filter_df = filter_outliers(merge_df)
            to_csv(filter_df, start_id, end_id)

    def getEstTime(self, start_id, end_id, weekday):
        """获取两点间通行时间"""
        filebase = '{}to{}'.format(start_id, end_id)
        dirpath = os.path.join(self.config.getConf(
            'analizeTime')['homepath'], filebase)
        filepath = os.path.join(dirpath, '{}-time.csv'.format(weekday))
        if not os.path.exists(filepath):
            self.est_cross_time(start_id, end_id)
        return pd.read_csv(filepath, names=['time_group', 'duration'], index_col=['time_group'])


class MainRoad(object):
    def __init__(self, estTime_df):
        self.config = Config()
        self.estTime_df = estTime_df
        database_conf = self.config.getConf('database')
        self.connection = pymysql.connect(
            database_conf['host'],
            database_conf['user'],
            database_conf['passwd'],
            database_conf['name']
        )

    def getGroupTime(self, time_group):
        return self.estTime_df.loc[time_group, 'duration']

    def getMainPath(self, start_id, end_id):
        def metaId_pass_cross(cross_id):
            query = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
                cross_id)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                traj_ids = cursor.fetchall()
            return traj_ids

        def traj_unique(meta_id, time, kind):
            if not kind in ['start', 'end']:
                print('filter_traj参数错误')
                exit()
            time_group = math.ceil((time.hour * 60 + time.minute) /
                                   int(self.config.getConf('analizeTime')['time_interval']))
            est_time = dt.timedelta(seconds=self.getGroupTime(time_group)) 
            max_time = 0
            if kind == 'start':
                max_time = time + est_time
            if kind == 'end':
                time, max_time = time-est_time, time

            query = "SELECT cross_id, time FROM traj_data WHERE metadata_id={} and time>={} and time<={}".format(
                meta_id, time, max_time)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                traj = cursor.fetchall()
            print(traj);exit()

        def filter_traj(start_meta_id):

            for meta_id, time in start_meta_id:
                traj_unique(meta_id, time, 'start')
                exit()

        start_meta_id = metaId_pass_cross(start_id)
        filter_traj(start_meta_id)


es = EstTime()
mr = MainRoad(es.getEstTime(52475, 52464, 'weekday'))
mr.getMainPath(52475, 52464)
