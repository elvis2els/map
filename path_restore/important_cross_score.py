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
        for vertices in self.map.g.vertices():





if __name__ == '__main__':
    shp_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp'
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    if os.path.exists(gt_file):
        file = gt_file
    else:
        file = shp_file
    r = RoadMap(file)
    r.load()
