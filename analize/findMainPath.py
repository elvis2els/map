#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import argparse
import datetime
import os
import time
import traceback

import matplotlib.pyplot as plt
import networkx as nx
import numpy as np
import pandas as pd
import pymysql

from Config import Config

parser = argparse.ArgumentParser(description="查找两路口之间的主通行路段")
parser.add_argument('start', help="起始路口号")
parser.add_argument('end', help="终止路口号")
parser.add_argument('weekday', choices=['weekday', 'weekend'], help="终止路口号")
parser.add_argument('-f', '--file', default='/home/elvis/map/analize/analizeTime/countXEntTime/',
                    help='存有同时通过两点间的metaid文件,文件为{output}/{start}to{end}/{start}to{end}{weekday}.csv,默认output为~/map/analize/analizeTime/countXEntTime/')
parser.add_argument('-o', '--output', default='/home/elvis/map/analize/analizePath',
                    help='输出文件路径,文件为{output}/{start}to{end}/{start}to{end}.csv,默认output为~/map/analize/analizePath/')
args = parser.parse_args()


def getMetadata_df():
    # 获取已经处理过的同时经过这两个路口的轨迹id
    filepath = os.path.join(args.file, '{}to{}'.format(args.start, args.end))
    filename = '{}.csv'.format(args.weekday)
    metaId_file = os.path.join(filepath, filename)
    if not os.path.exists(filepath):
        print('metaid文件不存在')
        exit()
    dateparse = lambda x: pd.datetime.strptime(x, '%Y-%m-%d %H:%M:%S.%f')
    df = pd.read_csv(metaId_file, parse_dates=[
                     'time_x', 'time_y'], date_parser=dateparse).drop(['duration', 'weekday'], axis=1)
    return df.sort_values(by='time_group')


def getTraj(connection, metadata):
    Query = "SELECT cross_id, time FROM traj_data WHERE metadata_id={}".format(
        metadata['meta_id'])
    with connection.cursor() as cursor:
        cursor.execute(Query)
        result = cursor.fetchall()
    traj_df = pd.DataFrame(list(result), columns=['cross_id', 'time'])
    return traj_df[(traj_df.time >= metadata['time_x']) & (traj_df.time <= metadata['time_y'])]


def addGraph(G, traj_df):
    # 轨迹存入Graph中
    cross_ids = list(traj_df['cross_id'])
    edges = [(s, e) for s, e in zip(cross_ids[:-1], cross_ids[1:])]
    for edge in edges:
        if G.has_edge(*edge):
            G[edge[0]][edge[1]]['weight'] += 1
        else:
            G.add_edge(edge[0], edge[1], weight=1)
    # G.add_path(list(traj_df['cross_id']))

def analizeSingleMainPath(connection, metadatas, timegroup):
    G = nx.DiGraph()
    metadata_timegroup = metadatas[metadatas.time_group == timegroup]
    if not metadata_timegroup.empty:
        for ix, metadata in metadata_timegroup.iterrows():
            traj_df = getTraj(connection, metadata)
            addGraph(G, traj_df)
            # drawGraph(G)

def drawGraph(G):
    pos = nx.fruchterman_reingold_layout(G)
    edge_labels = dict([((u,v,), d['weight']) for u,v,d in G.edges(data=True)])
    nx.draw(G, pos, with_labels=True)
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels)
    plt.show()

def analizeAllMainPath(connection, metadatas):
    first_timegroup, last_timegroup = metadatas[
        'time_group'].iloc[0], metadatas['time_group'].iloc[-1]
    for timegroup in range(first_timegroup, last_timegroup + 1):
        analizeSingleMainPath(connection, metadatas, timegroup)


def main():
    metadatas = getMetadata_df()

    conf = Config()
    connection = pymysql.connect(
        conf.dbhost, conf.dbuser, conf.dbpasswd, conf.dbname)
    print('#数据库连接成功')

    try:
        all_start_time = time.time()
        analizeAllMainPath(connection, metadatas)

        print('总时长: {}s'.format(time.time() - all_start_time))
    # except Exception as e:
    #     print('轨迹查询错误')
    #     print(e)
    finally:
        connection.close()

if __name__ == '__main__':
    main()
