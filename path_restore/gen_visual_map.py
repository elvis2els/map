import argparse
import os
import time
from concurrent import futures

import geopandas
import networkx as nx
import pandas as pd
import pymysql
import shapefile
from Config import Config
from DBUtils.PooledDB import PooledDB
from gt_roadmap import RoadMap
from shapely.geometry import LineString

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)

parser = argparse.ArgumentParser(description="查找两地标间主路段，并计算通行时间，使用前需先清空表visual_edge")
parser.add_argument('filter', help='仅保留前x个地标')
args = parser.parse_args()


def readShp(shpfile):
    sf = shapefile.Reader(shpfile)
    records = sf.records()
    max_rank = int(args.filter)
    if max_rank > len(records):
        max_rank = len(records)
    return pd.DataFrame.from_records(records, columns=['cross_index', 'entropy', 'rank'], index='rank')[:max_rank]


def chunks(l, n):
    for i in range(0, len(l), n):
        yield l[i:i + n]


def projection(metaIds):
    print('开始进程({}), size={}'.format(os.getpid(), len(metaIds)))
    conn = pymysql.connect(database_conf['host'], database_conf[
        'user'], database_conf['passwd'], database_conf['name'])
    traj_df = pd.DataFrame(columns=['start_id', 'end_id'])
    with conn.cursor() as cursor:
        for metaId in metaIds:
            query_traj = 'SELECT cross_id FROM traj_data WHERE metadata_id={}'.format(
                metaId)
            cursor.execute(query_traj)
            traj = cursor.fetchall()
            ret_traj = []
            for pass_cross in traj:
                if pass_cross[0] in main_cross:
                    ret_traj.append(pass_cross[0])
            if len(ret_traj) > 1:
                traj_tmp_df = pd.DataFrame(
                    {'start_id': ret_traj[:-1], 'end_id': ret_traj[1:]})
                traj_df = pd.concat([traj_df, traj_tmp_df], ignore_index=True)
    print(traj_df.head())
    return traj_df


def traj2mainCross():
    traj_all_df = traj_df = pd.DataFrame(columns=['start_id', 'end_id'])
    with connection.cursor() as cursor:
        print('find all id ...')
        query_metaId = 'SELECT id FROM traj_metadata'
        cursor.execute(query_metaId)
        metaIds = cursor.fetchall()
        metaIds = chunks([x[0] for x in metaIds], 100000)

    with futures.ProcessPoolExecutor() as pool:
        i = 1
        for traj_df in pool.map(projection, metaIds):
            print('完成{}'.format(i))
            i += 1
            traj_all_df = pd.concat([traj_all_df, traj_df], ignore_index=True)

    print('traj2mainCross finish!')
    return traj_all_df


def gen_graph(traj_df):
    print('开始grouped.....')
    s_t = time.time()
    grouped = traj_df.groupby(['start_id', 'end_id']).apply(len)
    grouped = grouped[grouped >= int(config.getConf(
        'analizeTime')['edge_pass_number'])]
    print('完成grouped: {}s'.format(time.time() - s_t))
    # grouped = grouped[grouped >= 5]
    G = nx.DiGraph()
    i, maxlen, start_time = 1, len(grouped), time.time()
    for cross, weight in grouped.iteritems():
        if time.time() - start_time > 1:
            print('{}/{}'.format(i, maxlen), end='\r')
            start_time = time.time()
        i += 1
        start_id, end_id = cross[0], cross[1]
        if start_id == end_id:
            continue
        if G.has_edge(start_id, end_id):
            G[start_id][end_id]['weight'] += weight
        else:
            G.add_edge(start_id, end_id, weight=weight)
    return G


def edge_to_mysql(G):
    with connection.cursor() as cursor:
        insert_query = 'INSERT INTO visual_edge_odgroup (start_cross_id, end_cross_id, weight) values(%s, %s, %s);'
        values = []
        for edge in G.edges_iter(data='weight'):
            values.append((int(edge[0][:-2]), int(edge[1][:-2]), int(edge[2])))
        cursor.executemany(insert_query, values)
    connection.commit()


def edge_to_shp(G):
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    geo_map = RoadMap(gt_file)
    geo_map.load()
    edge_geo = []
    for edge in G.edges_iter():
        start_cross, end_cross = int(edge[0][:-2]), int(edge[1][:-2])
        start_pos, end_pos = geo_map.g.vertex_properties['pos'][start_cross], geo_map.g.vertex_properties['pos'][end_cross]
        edge_geo.append((start_cross, end_cross, LineString([start_pos, end_pos])))
    df = pd.DataFrame(edge_geo, columns=['start_cross', 'end_cross', 'geometry'])
    visual_map_df = geopandas.GeoDataFrame(df, geometry='geometry')
    file_path = '/home/elvis/map/analize/analizeCross/visualmap.shp'
    visual_map_df.to_file(file_path)


config = Config()
database_conf = config.getConf('database')
connection = pymysql.connect(database_conf['host'], database_conf[
    'user'], database_conf['passwd'], database_conf['name'])
shpfile = config.getConf('analizeTime')['cross_file']
shp_df = readShp(shpfile)
main_cross = set(shp_df['cross_index'])


def main():
    # traj_df = traj2mainCross()
    # G = gen_graph(traj_df)
    dirpath = config.getConf('analizeTime')['homepath']
    # nx.write_gexf(G, os.path.join(dirpath, 'visualMapTop{}.gexf'.format(args.filter)))
    G = nx.read_gexf(os.path.join(dirpath, 'visualMapTop{}.gexf'.format(args.filter)))
    # edge_to_mysql(G)
    edge_to_shp(G)


if __name__ == '__main__':
    start_time = time.time()
    main()
    print('总用时：{}'.format(time.time() - start_time))
