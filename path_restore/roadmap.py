import shapefile

from enum import Enum
from scipy.spatial import distance

class Direction(Enum):
    Bidirection = 0
    Forward = 2


class RoadSegment(object):
    speed_limit = {4: 80, 3: 60, 2: 40}

    def __init__(self):
        self.index = -1
        self.start_cross_index = -1
        self.end_cross_index = -1
        self.direction = -1
        self.geometry = []
        self.road_class = -1

    def getSpeedLimit(self):
        if self.road_class == -1:
            return 0
        return speed_limit[self.road_class] / 3.6

    def __str__(self):
        return '{} {}'.format(self.start_cross_index, self.end_cross_index)


class Roadmap(object):

    def __init__(self, mapfile):
        self._mapfile = mapfile
        self._roads = []
        self._cross = []
        self.DIRECTION_index = 6
        self.PATHCLASS_index = 20

    def load(self):
        try:
            sf = shapefile.Reader(self._mapfile)
        except:
            return False
        roads_records = sf.shapeRecords()  # 获取路段信息'
        cross_map = {}
        for road_record in roads_records:
            road = RoadSegment()
            road.index = len(self._roads)
            road.geometry = road_record.shape.points
            road.direction = road_record.record[self.DIRECTION_index]
            road.road_class = road_record.record[self.PATHCLASS_index]
            cross_s = road_record.shape.points[0]
            cross_e = road_record.shape.points[-1]
            if not cross_s in cross_map:
                cross_map[cross_s] = road.start_cross_index = len(self._cross)
                self._cross.append(cross_s)
            else:
                road.start_cross_index = cross_map[cross_s]
            if not cross_e in cross_map:
                cross_map[cross_e] = road.end_cross_index = len(self._cross)
                self._cross.append(cross_e)
            else:
                road.end_cross_index = cross_map[cross_e]
            self._roads.append(road)
        return True

    def getCross(self, index):
        return self._cross[index]

    def getRoadByCross(self, cross_start_index, cross_end_index):
        for road_index, road in enumerate(self._roads):
            if int(road.direction) == Direction['Bidirection'].value:
                ends_cross = [road.start_cross_index, road.end_cross_index]
                if cross_start_index in ends_cross and cross_end_index in ends_cross:
                    return road
            else:
                if cross_start_index == road.start_cross_index and cross_end_index == road.end_cross_index:
                    return road
        return None

    def getRoadSpeedLimit(self, road_index):
        return self._roads[road_index].getSpeedLimit()

    def distance_cross(self, cross1_index, cross2_index):
        return distance.euclidean(self.getCross(cross1_index), self.getCross(cross2_index))


if __name__ == '__main__':
    import time
    time_s = time.time()
    a = Roadmap('/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp')
    # a = Roadmap('/Users/heyulong/Downloads/Beijing2011/bj-road-epsg3785.shp')
    a.load()
    print(time.time()-time_s)