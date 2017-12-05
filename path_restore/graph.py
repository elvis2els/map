#!/usr/bin/python3
# -*- coding: UTF-8 -*-
from graph_tool import Graph
from graph_tool.topology import all_paths, shortest_distance
import time


class MapGraph(object):
    def __init__(self):
        self.g = Graph()
        self.dvertex_index = dict()
        self.vertex_label = self.g.new_vertex_property("string")
        self.g.vertex_properties["label"] = self.vertex_label
        self.edge_weight = self.g.new_edge_property("int")
        self.g.edge_properties["weight"] = self.edge_weight

    def has_vertex(self, label):
        """返回index"""
        return self.dvertex_index.get(label)

    def has_edge(self, s_label, e_label):
        s_vertex = self.dvertex_index.get(s_label)
        e_vertex = self.dvertex_index.get(e_label)
        if s_vertex and e_vertex:
            return self.g.edge(s_vertex, e_vertex)
        else:
            return None

    def add_edge(self, s_label, e_label):
        if self.has_edge(s_label, e_label):
            return self.g.edge(s_label, e_label)
        s_vertex = self.add_vertex(s_label)
        e_vertex = self.add_vertex(e_label)
        return self.g.add_edge(s_vertex, e_vertex)

    def add_vertex(self, label):
        """如果点存在则直接返回节点索引号"""
        if self.dvertex_index.get(label):
            return self.dvertex_index.get(label)
        v = self.g.add_vertex()
        self.vertex_label[v] = label
        self.dvertex_index[label] = v
        return v

    def add_edge_weight(self, s_label, e_label, weight):
        if self.has_edge(s_label, e_label):
            self.edge_weight[self.g.edge(s_label, e_label)] += weight
        else:
            edge = self.add_edge(s_label, e_label)
            self.edge_weight[edge] = weight

    @classmethod
    def networkx_to_graph_tool(cls, nx_g):
        gt_g = MapGraph()
        for e in nx_g.edges():
            gt_g.add_edge_weight(e[0], e[1], nx_g[e[0]][e[1]]["weight"])
        return gt_g

    def all_paths(self, s_label, e_label):
        if self.has_vertex(s_label) and self.has_vertex(e_label):
            time_s = time.time()
            s_vertex = self.dvertex_index.get(s_label)
            e_vertex = self.dvertex_index.get(e_label)
            for path in all_paths(self.g, s_vertex, e_vertex,
                                  cutoff=shortest_distance(self.g, s_vertex, e_vertex) * 1.5):
                if time.time() - time_s > 60*10:
                    break
                yield path
