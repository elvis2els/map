#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import os

import numpy as np
import pandas as pd
from gt_roadmap import RoadMap
from progressbar import Percentage, Bar, ETA, ProgressBar
from shapely.geometry import Point

from sql_about.MysqlDB import MysqlDB
import geopandas


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

    def compute_all_cross(self, type_name):
        type_id = self.get_type_id(type_name)
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
        count_od_groups['p'] = count_od_groups['count'] / sum_count
        # od_group_score = -count_od_groups['count'] * count_od_groups['p'] * np.log2(count_od_groups['p'])
        od_group_score = -count_od_groups['p'] * np.log2(count_od_groups['p'])
        return od_group_score.sum() * sum_count

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

    def get_score(self, score_type):
        """获取计算出的结果"""
        query = None
        if score_type == 'od_group':
            query = "SELECT cross_id, score FROM visual_cross WHERE type=3 ORDER BY score DESC"
        elif score_type == 'od_group_v2':
            query = "SELECT cross_id, score FROM visual_cross WHERE type=4 ORDER BY score DESC"
        elif score_type == 'od_group*entropy':
            query = """SELECT
                                    b.cross_id,
                                    b.od_group * b.entropy AS score
                                FROM
                                    (SELECT
                                        a0.cross_id,
                                        a0.score          AS od_group,
                                        CASE WHEN a1.score IS NULL
                                        THEN 0
                                        ELSE a1.score END AS entropy
                                    FROM (
                                            SELECT
                                                cross_id,
                                                type,
                                                score
                                            FROM visual_cross
                                            WHERE type = 3
                                            ) AS a0 LEFT JOIN (SELECT
                                                                cross_id,
                                                                type,
                                                                score
                                                            FROM visual_cross
                                                            WHERE type = 1) AS a1
                                            ON a0.cross_id = a1.cross_id) AS b
                                ORDER BY score DESC"""
        cross_scores = pd.read_sql_query(query, self.db.connection())
        cross_scores['rank'] = cross_scores.index
        return cross_scores

    def write_to_shp(self, path, df):
        df['geometry'] = df.apply(lambda x: Point(self.map.g.vertex_properties['pos'][x.cross_id]), axis=1)
        cross_scores = geopandas.GeoDataFrame(df, geometry='geometry')
        cross_scores.to_file(path)


if __name__ == '__main__':
    shp_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp'
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    if os.path.exists(gt_file):
        file = gt_file
    else:
        file = shp_file
    important_cross = ImportantCorss(file)
    # important_cross.important_score(11947)
    df = important_cross.get_score(score_type='od_group_v2')
    important_cross.write_to_shp('/home/elvis/map/analize/analizeCross/od_group*entropy_v2.shp', df)
    # important_cross.compute_all_cross(type_name='count*od_group_v2')
