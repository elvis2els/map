import argparse
import math
import os
import time
from concurrent import futures

import networkx as nx
import pandas as pd
import pymysql
import shapefile

from Config import Config

parser = argparse.ArgumentParser(description="查找两地标间主路段，并计算通行时间")
parser.add_argument('filter', help='仅保留前x个地标')
args = parser.parse_args()


def readShp(shpfile):
    sf = shapefile.Reader(shpfile)
    records = sf.records()
    max_rank = int(args.filter)
    if max_rank > len(records):
        max_rank = len(records)
    return pd.DataFrame.from_records(records, columns=['rank', 'cross_index', 'count', 'entropy'], index='rank')[:max_rank]


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

config = Config()
database_conf = config.getConf('database')
connection = pymysql.connect(database_conf['host'], database_conf[
                             'user'], database_conf['passwd'], database_conf['name'])
shpfile = config.getConf('analizeTime')['cross_file']
shp_df = readShp(shpfile)
main_cross = set(shp_df['cross_index'])


def main():
    traj_df = traj2mainCross()
    G = gen_graph(traj_df)
    dirpath = config.getConf('analizeTime')['homepath']
    nx.write_gexf(G, os.path.join(dirpath, 'visual_map.gexf'))

if __name__ == '__main__':
    start_time = time.time()
    main()
    print('总用时：{}'.format(time.time() - start_time))
