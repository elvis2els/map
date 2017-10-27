#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import os

from DBUtils.PooledDB import PooledDB

from path_restore.gt_roadmap import RoadMap
from sql_about.MysqlDB import MysqlDB


class ImportantCorss(object):
    def __init__(self, map_file):
        self.map = RoadMap(map_file)
        self.map.load()
        self.db = MysqlDB(5, host='192.168.3.199', user='root', passwd='123456', db='path_restore')

    def compute_all_cross(self):
        for cross in self.map.g.vertices():
            pass

    def important_score(self, cross):
        with self.db.cursor() as cursor:
            query = """SELECT
                            c.od_group,
                            count(c.metadata_id)
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
                    GROUP BY c.od_group""".format(cross_id=cross)
            cursor.execute(query)


if __name__ == '__main__':
    shp_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp'
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    if os.path.exists(gt_file):
        file = gt_file
    else:
        file = shp_file
    r = RoadMap(file)
    r.load()
