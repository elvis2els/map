#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import os
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from gt_roadmap import RoadMap
from sql_about.MysqlDB import MysqlDB
from progressbar import Percentage, Bar, ETA, FileTransferSpeed, ProgressBar


class ImportantCorss(object):
    def __init__(self, map_file):
        self.map = RoadMap(map_file)
        self.map.load()
        self.db = MysqlDB(5, host='192.168.3.199', user='root', passwd='123456', db='path_restore')

    # def compute_all_cross(self):
    #     type_id = self.get_type_id('count*od_group')
    #     with ProcessPoolExecutor(max_workers=1) as executor:
    #         futures = {executor.submit(self.important_score, int(cross.__str__())): int(cross.__str__()) for cross in
    #                    self.map.g.vertices()}
    #         for future in as_completed(futures):
    #             cross_id = futures[future]
    #             try:
    #                 score = future.result()
    #                 self.score_to_sql(cross_id, score, type_id)
    #             except Exception as e:
    #                 print(str(e))

    def compute_all_cross(self):
        type_id = self.get_type_id('count*od_group')
        sum_num = self.map.g.num_vertices()
        widgets = ['Insert: ', Percentage(), ' ', Bar(marker='#'), ' ', ETA()]
        pbar = ProgressBar(widgets=widgets, maxval=sum_num).start()
        for cross in self.map.g.vertices():
            cross_id = int(cross.__str__())
            try:
                score = self.important_score(cross_id)
                self.score_to_sql(cross_id, score, type_id)
                pbar.update(cross_id)
            except Exception as e:
                print(str(e))

    def important_score(self, cross_id):
        """计算路口重要性分数"""
        query = """SELECT
                            c.od_group,
                            count(c.metadata_id) AS count
                    FROM (SELECT
                            a.metadata_id,
                            b.od_group
                        FROM (SELECT DISTINCT metadata_id
                                FROM traj_data
                                WHERE cross_id = {cross_id}) AS a
                            JOIN (SELECT
                                    id,
                                    od_group
                                FROM traj_metadata
                                WHERE od_group IS NOT NULL) AS b ON a.metadata_id = b.id) AS c
                    GROUP BY c.od_group""".format(cross_id=cross_id)
        count_od_groups = pd.read_sql_query(query, self.db.connection())
        sum_count = count_od_groups['count'].sum()
        p = count_od_groups['count'] / sum_count
        od_group_score = -count_od_groups['count'] * p * np.log2(p)
        return od_group_score.sum()

    def get_type_id(self, visual_type):
        """获取type对应id"""
        with self.db.cursor() as cursor:
            query = "SELECT id FROM visual_type WHERE type_name = '{}'".format(visual_type)
            cursor.execute(query)
            type_id = cursor.fetchone()[0]
            return type_id

    def score_to_sql(self, cross_id, score, type_id):
        """计算好的结果存入数据库"""
        with self.db.cursor() as cursor:
            insert = """INSERT INTO visual_cross (cross_id, type, score) VALUES ({cross_id},{type_id},{score}) 
                        ON DUPLICATE KEY UPDATE score={score}""".format(cross_id=cross_id, score=score, type_id=type_id)
            cursor.execute(insert)


if __name__ == '__main__':
    shp_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp'
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    if os.path.exists(gt_file):
        file = gt_file
    else:
        file = shp_file
    important_cross = ImportantCorss(file)
    important_cross.compute_all_cross()
