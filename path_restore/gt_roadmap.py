import shapefile
from graph_tool import Graph, load_graph
from graph_tool.draw import graph_draw
import os

from scipy.spatial import distance


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


class RoadMap(object):
    def __init__(self, mapfile):
        self._mapfile = mapfile
        self.DIRECTION_index = 6
        self.PATHCLASS_index = 20
        self.g = Graph()
        self.g.edge_properties["length"] = self.g.new_edge_property("double")
        self.g.edge_properties["level"] = self.g.new_edge_property("int")
        self.g.vertex_properties["pos"] = self.g.new_vertex_property("vector<double>")
        self.cross_pos_index = {}

    def load(self):
        if self._mapfile[-3:] != 'shp':
            self.g = load_graph(self._mapfile)
            return

        try:
            sf = shapefile.Reader(self._mapfile)
        except Exception as e:
            print(str(e))
            return False
        roads_records = sf.shapeRecords()  # 获取路段信息'
        for road_record in roads_records:
            cross_s_index = self.add_cross(road_record.shape.points[0])
            cross_e_index = self.add_cross(road_record.shape.points[-1])
            self.add_road_edge(cross_s_index, cross_e_index, road_record)
            if int(road_record.record[self.DIRECTION_index]) == 0:  # 若路段是双向车道
                self.add_road_edge(cross_e_index, cross_s_index, road_record)
        return True

    def has_edge(self, s_vertex, e_vertex):
        if self.g.num_vertices() >= max(s_vertex, e_vertex):
            return self.g.edge(s_vertex, e_vertex)
        else:
            return None

    def add_cross(self, cross_pos):
        if cross_pos in self.cross_pos_index:
            return self.cross_pos_index.get(cross_pos)
        else:
            cross_index = self.g.add_vertex()
            self.g.vp.pos[cross_index] = cross_pos
            self.cross_pos_index[cross_pos] = cross_index
            return cross_index

    def add_road_edge(self, s_vertex, e_vertex, road):
        if self.has_edge(s_vertex, e_vertex):
            return self.g.edge(s_vertex, e_vertex)
        else:
            edge = self.g.add_edge(s_vertex, e_vertex)
            self.g.ep.level[edge] = int(road.record[self.PATHCLASS_index])
            self.g.ep.length[edge] = self.road_length(road)
            return edge

    @staticmethod
    def road_length(road):
        length = 0
        for sub_road in zip(road.shape.points[:-1], road.shape.points[1:]):
            length += distance.euclidean(sub_road[0], sub_road[1])
        return length


if __name__ == '__main__':
    shp_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.shp'
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    if os.path.exists(gt_file):
        file = gt_file
    else:
        file = shp_file
    r = RoadMap(file)
    r.load()
    graph_draw(r.g, pos=r.g.vp.pos, output_size=(600, 600), output='/home/elvis/图片/2017-10-12/2.svg')
    # graph_draw(r.g, pos=r.g.vp.pos)