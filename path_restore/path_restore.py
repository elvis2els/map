#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import datetime as dt
import math
import os
import time

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import pymysql

from Config import Config
from roadmap import Roadmap
from path_restore.graph import MapGraph


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

        def valueList(df, metaid, weekday):
            values = []
            for time_group, duration in df.iteritems():
                values.append((str(metaid), '%.5f' %
                               duration, str(weekday), str(time_group)))
            return values

        def to_mysql(df, start_id, end_id):
            df_weekday = df[df['weekday'] < 6]
            df_weekend = df[df['weekday'] >= 6]
            grouped_weekday = df_weekday[
                'duration'].groupby(df['time_group']).mean()
            grouped_weekend = df_weekend[
                'duration'].groupby(df['time_group']).mean()
            with self.connection.cursor() as cursor:
                getId_query = 'SELECT id FROM visual_edge WHERE start_cross_id={} and end_cross_id={}'.format(
                    start_id, end_id)
                cursor.execute(getId_query)
                metaId = cursor.fetchone()[0]
                insert_query = 'INSERT INTO cross_pre_time (edge_meta_id, duration, weekday, time_group) VALUES (%s, %s, %s, %s);'
                values = valueList(grouped_weekday, metaId, 1)
                cursor.executemany(insert_query, values)
                values = valueList(grouped_weekend, metaId, 0)
                cursor.executemany(insert_query, values)
            self.connection.commit()

        filebase = '{}to{}'.format(start_id, end_id)
        dirpath = os.path.join(self.config.getConf(
            'analizeTime')['homepath'], filebase)
        filepath = os.path.join(dirpath, '{}.csv'.format(filebase))
        if not os.path.exists(filepath):
            merge_df = merged_trajId(start_id, end_id)
            filter_df = filter_outliers(merge_df)
            # to_csv(filter_df, start_id, end_id)
            to_mysql(filter_df, start_id, end_id)

    def getEstTime(self, start_id, end_id, weekday=True):
        """获取两点间通行时间"""
        with self.connection.cursor() as cursor:
            query = 'SELECT id FROM visual_edge WHERE start_cross_id={} AND end_cross_id={}'.format(
                start_id, end_id)
            cursor.execute(query)
            metaid = cursor.fetchone()
            if len(metaid) == 0:
                return pd.DataFrame(columns=['time_group', 'duration'])
            metaid = metaid[0]

        if weekday:
            weekday = 1
        else:
            weekday = 0
        query = 'SELECT time_group, duration FROM cross_pre_time WHERE edge_meta_id={} AND weekday={}'.format(
            metaid, weekday)
        return pd.read_sql_query(query, self.connection, index_col=['time_group'])


class MainRoad(object):
    def __init__(self, road, show_detail=False):
        self.config = Config()
        self.estTime = EstTime()
        self.road = road
        self.show_detail = show_detail
        database_conf = self.config.getConf('database')
        self.connection = pymysql.connect(
            database_conf['host'],
            database_conf['user'],
            database_conf['passwd'],
            database_conf['name']
        )
        # if not self.road.load():
        #     print('map shp load error!')
        #     exit()
        # print('map load done!')

    def getMainPath(self, start_id, end_id, weekday=True):
        """得到两点间主路段"""

        def metaId_pass_cross(cross_id):
            """获取通过指定路口的轨迹id和时间"""
            query = "SELECT metadata_id, time FROM traj_data WHERE cross_id={}".format(
                cross_id)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                traj_ids = cursor.fetchall()
            return traj_ids

        def getGroupTime(time_group):
            """获取指定时间分组的预估计时间"""
            try:
                groupTime = estTime_df.loc[time_group, 'duration']
            except KeyError as e:  # 有可能time_group不在ESTTime_df的time_group里
                return 0
            return groupTime

        def traj_in_time(meta_id, time, kind, space_limit):
            """获取指定轨迹id在有效时间区间内的轨迹"""
            if not kind in ['start', 'end']:
                print('filter_traj参数错误')
                exit()

            time_group = math.ceil((time.hour * 60 + time.minute) /
                                   int(self.config.getConf('analizeTime')['time_interval']))
            est_time = dt.timedelta(seconds=getGroupTime(
                time_group) * float(self.config.getConf('analizeTime')['time_factor']))
            if est_time == 0:
                return pd.DataFrame(columns=['start_id', 'end_id', 'time_group'])

            max_time = 0
            if kind == 'start':
                max_time = time + est_time
            if kind == 'end':
                time, max_time = time - est_time, time

            query = "SELECT cross_id FROM traj_data WHERE metadata_id={} and time>=\"{}\" and time<=\"{}\"".format(
                meta_id, time, max_time)
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                traj = cursor.fetchall()
            cross_id = [c_id[0] for c_id in traj]
            # if kind == 'end':
            #     cross_id = cross_id[::-1]      #统一成start来处理
            # valid_cross = []
            # for index in cross_id:       #需要裁取空间区域内的部分
            #     if not space_limit.isInSpace(self.road.getCross(index)):
            #         break
            #     valid_cross.append(index)
            # print(valid_cross);exit()
            # if kind == 'end':
            #     valid_cross = valid_cross[::-1]
            for index in cross_id:
                if not space_limit.isInSpace(self.road.getCross(index)):
                    return pd.DataFrame(columns=['start_id', 'end_id', 'time_group'])
            return pd.DataFrame({'start_id': cross_id[:-1], 'end_id': cross_id[1:], 'time_group': time_group})

        def drawGraph(G):
            pos = nx.spring_layout(G)
            edge_labels = dict([((u, v,), d['weight'])
                                for u, v, d in G.edges(data=True)])
            nx.draw(G, pos, with_labels=True)
            nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
            plt.show()

        def add_graph(G, df):
            for cross, weight in df.iteritems():
                if cross[0] == cross[1]:
                    continue
                edge = (cross[0], cross[1])
                if G.has_edge(*edge):
                    G[edge[0]][edge[1]]['weight'] += weight
                else:
                    G.add_edge(edge[0], edge[1], weight=weight)

        def gen_graph(traj_start_df, traj_end_df, time_group):
            """根据指定的时间分组生成图"""
            graph = nx.DiGraph()
            for traj_df in [traj_start_df, traj_end_df]:
                traj_group_df = traj_df[traj_df['time_group'] == time_group]
                gb = traj_group_df.groupby(['start_id', 'end_id']).apply(len)
                add_graph(graph, gb)
            return graph

        def get_traj(meta_ids, kind):
            """根据指定轨迹id集得到符合合理时间区间的轨迹集合"""
            if not kind in ['start', 'end']:
                print('filter_traj参数错误')
                exit()
            traj_df = pd.DataFrame(
                columns=['start_id', 'end_id', 'time_group'])

            spaceLimit = SpaceLimit(self.road, start_id, end_id)
            i, maxlen, s_time = 1, len(meta_ids), time.time()
            for meta_id, traj_time in meta_ids:
                if self.show_detail:
                    if time.time() - s_time > 1:
                        print("{}/{}".format(i, maxlen), end='\r')
                        s_time = time.time()
                    i += 1
                traj_df = pd.concat([traj_df, traj_in_time(
                    meta_id, traj_time, kind, spaceLimit)], ignore_index=True)
            return traj_df

        def getPathWeight(graph, path):
            """获取path中路径权值并排序"""
            return sorted([graph.g.edge_properties["weight"][graph.g.edge(s, e)] for s, e in zip(path[:-1], path[1:])])

        def MFP(graph):
            """使用MFP查找主路径"""
            mfp = mfp_w = []
            i = 0
            graph = MapGraph.networkx_to_graph_tool(graph)
            for path in graph.all_paths(start_id, end_id):
                i += 1
                mfp_w_tmp = getPathWeight(graph, path)
                if not mfp:
                    mfp = path
                    mfp_w = mfp_w_tmp
                else:
                    if mfp_w < mfp_w_tmp:
                        mfp = path
                        mfp_w = mfp_w_tmp
            mfp = [int(m) for m in mfp]
            return mfp

        def filter_graph(graph):
            from_s = nx.descendants(graph, start_id)
            from_s.add(start_id)
            to_e = nx.ancestors(graph, end_id)
            to_e.add(end_id)
            del_cross = (from_s | to_e) - (from_s & to_e)
            graph.remove_nodes_from(del_cross)

        def find_main_path(start_meta_id, end_meta_id):
            """分别生成s出发和e到达的图，合并有效部分,根据合成的部分得到主路径"""
            traj_start_df = get_traj(start_meta_id, 'start')
            traj_end_df = get_traj(end_meta_id, 'end')
            time_groups = estTime_df.index.tolist()
            mfp_all = []
            for time_group in time_groups:
                # print(time_group)
                graph = gen_graph(traj_start_df, traj_end_df, time_group)
                if len(graph) == 0 or not graph.has_node(start_id) or not graph.has_node(
                        end_id):  # 可能出现某一时段没有车辆通过这两个路口且无法拼接
                    continue
                # if time_group == 74:
                #     drawGraph(G)
                filter_graph(graph)
                # if time_group == 74:
                #     drawGraph(G)
                # G = nx.DiGraph()
                # print('time_group:{} node:{} edge:{}'.format(time_group, G.number_of_nodes(), G.size()))
                if len(graph) == 0:  # 可能出现某一时段过滤后没有车辆通过这两个路口且无法拼接
                    continue
                mfp = MFP(graph)
                if not mfp_all:
                    mfp_all.append([[time_group, time_group], mfp])
                else:
                    if mfp_all[-1][0][1] + 1 == time_group and mfp_all[-1][1] == mfp:
                        mfp_all[-1][0][1] = time_group
                    else:
                        mfp_all.append([[time_group, time_group], mfp])
            return mfp_all

        estTime_df = self.estTime.getEstTime(start_id, end_id, weekday=weekday)
        start_meta_id = metaId_pass_cross(start_id)
        end_meta_id = metaId_pass_cross(end_id)
        return find_main_path(start_meta_id, end_meta_id)


class SpaceLimit(object):
    def __init__(self, road, start_index, end_index):
        roadlenth = road.distance_cross(start_index, end_index) * 0.75
        coord_s = road.getCross(start_index)
        coord_e = road.getCross(end_index)
        if coord_s[0] > coord_e[0]:
            coord_s, coord_e = coord_e, coord_s
        self.left, self.right = coord_s[0] - roadlenth, coord_e[0] + roadlenth
        if coord_s[1] > coord_e[1]:
            coord_s, coord_e = coord_e, coord_s
        self.down, self.top = coord_s[1] - roadlenth, coord_e[1] + roadlenth

    def isInSpace(self, coord):
        return coord[0] >= self.left and coord[0] <= self.right and coord[1] >= self.down and coord[1] <= self.top

# 测试用
# es = EstTime()
# es.getEstTime(53112, 65156, weekday=True)
# mr = MainRoad(
#     '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp', show_detail=True)
# print(mr.getMainPath(53112, 65156, weekday=True))
