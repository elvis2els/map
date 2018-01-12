import copy
import datetime as dt
import os
import sys
import time
import traceback
from datetime import timedelta
from queue import PriorityQueue

import networkx as nx
import numpy as np
import pymysql
from DBUtils.PooledDB import PooledDB
from gt_roadmap import RoadMap
import skcriteria as sk
from skcriteria.madm import closeness

from path_restore import EstTime

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


class TestPath(object):
    def __init__(self, metadata_id, start_index, count):
        self.metadata_id = metadata_id
        self.start_index = start_index
        self.count = count
        self.path_time = self.get_path(start_index, count)

    def get_path(self, start_index, count):
        file_path = '/home/elvis/map/analize/path_restore/raw_path/{i}-{s}-{c}'.format(i=self.metadata_id,
                                                                                       s=self.start_index, c=count)
        if os.path.exists(file_path):
            with open(file_path, 'r') as f:
                path_time = []
                paths_time_str = f.readlines()
                for path_time_str in paths_time_str:
                    path_time_str = path_time_str.strip('\n').split(',')
                    try:
                        path_time.append(
                            (int(path_time_str[0]), dt.datetime.strptime(path_time_str[1], '%Y-%m-%d %H:%M:%S.%f')))
                    except Exception as e:
                        path_time.append(
                            (path_time_str[0], dt.datetime.strptime(path_time_str[1], '%Y-%m-%d %H:%M:%S')))
                return tuple(path_time)
        conn = pool.connection()
        cursor = conn.cursor()
        try:
            query = """SELECT cross_id, time FROM traj_data 
                WHERE metadata_id={meta_id} LIMIT {start}, {count}""".format(meta_id=self.metadata_id,
                                                                             start=start_index,
                                                                             count=count)
            cursor.execute(query)
            path_time = cursor.fetchall()
            self.to_csv(file_path, path_time)
            return path_time
        except Exception as e:
            traceback.print_exc(e)
        finally:
            cursor.close()
            conn.close()

    @staticmethod
    def to_csv(file_path, path_time):
        result = ""
        for cross, cross_time in path_time:
            result += "{},{}\n".format(cross, cross_time)
        with open(file_path, 'w') as f:
            f.write(result)


def get_pre_time(start_cross, end_cross, start_cross_time):
    if start_cross == end_cross:
        return 0
    start_time = start_cross_time.time()
    time_group = int(start_time.hour * 4 + start_time.minute / 15)
    weekday = 1 if start_cross_time.weekday() < 5 else 0
    query = """SELECT cost
            FROM cross_pre_time_all
            WHERE start_cross = {s} AND end_cross = {e} AND time_group = {time_group} AND weekday = {weekday}
            """.format(s=start_cross, e=end_cross, time_group=time_group, weekday=weekday)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        cost = cursor.fetchone()
        if cost is not None and len(cost) > 0:
            return float(cost[0])
        else:
            query_exist = """SELECT id FROM cross_pre_time_all 
                WHERE start_cross = {s} AND end_cross = {e}""".format(s=start_cross, e=end_cross)
            cursor.execute(query_exist)
            if cursor.fetchone() is not None:
                return False
            est = EstTime()
            est.est_cross_time(start_cross, end_cross)
            return True
    except Exception as e:
        print('start_cross:{s} end_cross:{e} is not find'.format(s=start_cross, e=end_cross))
        return False
    finally:
        cursor.close()
        conn.close()


def get_visual_meta_id(start_cross, end_cross):
    query = """SELECT id FROM visual_edge_odgroup 
        WHERE start_cross_id={s} AND end_cross_id={e}""".format(s=start_cross, e=end_cross)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        traceback.print_exc(e)
    finally:
        cursor.close()
        conn.close()


def get_cost(start_cross, end_cross, time_group, weekday):
    meta_id = get_visual_meta_id(start_cross, end_cross)
    query = """SELECT cost FROM visual_edge_cost_filter 
                WHERE edge_meta_id={meta_id} AND time_group={time_group} 
                AND weekday={weekday}""".format(meta_id=meta_id, time_group=time_group, weekday=weekday)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        return cursor.fetchone()[0]
    except Exception as e:
        return None
    finally:
        cursor.close()
        conn.close()


def find_paths(start_cross, end_cross, visual_map, k=0.3):
    delta_time = (end_cross[1] - start_cross[1]).total_seconds()
    paths = []
    frontier = PriorityQueue()
    frontier.put((delta_time, start_cross[0], start_cross[1], [start_cross], 0))  # 分别代表与目标时间差值,节点号,时间,路径,实际消耗时间

    while not frontier.empty() and len(paths) < 10:
        current = frontier.get()
        if current[1] == end_cross[0]:
            paths.append([current[3], current[4]])
            if frontier.empty():
                break
            else:
                continue
        current_time = current[2]
        current_time_group = int(current_time.time().hour * 4 + current_time.time().minute / 15)
        weekday = 1 if current_time.weekday() < 5 else 0
        for next_cross in visual_map.neighbors_iter(str(float(current[1]))):  # 计算每个出边
            next_cross = int(float(next_cross))
            if next_cross in current[3]:
                continue
            c_to_n_cost = get_cost(current[1], next_cross, current_time_group, weekday)
            if c_to_n_cost is None:
                continue
            new_cost = current[4] + c_to_n_cost
            new_time = current_time + timedelta(seconds=c_to_n_cost)
            pre_time_est = get_pre_time(int(float(next_cross)), end_cross[0], new_time)
            if pre_time_est is True:  # 数据库中没有该估值,执行估值操作后再运行该步骤
                pre_time_est = get_pre_time(int(float(next_cross)), end_cross[0], new_time)
            if pre_time_est is False:  # 经过粗略估算扔无法得到该值
                # total_cost_est = (1 - k) * delta_time  # 将评估优先级降到合理值的最低
                continue  # 认为这样的路口是不合理的
            else:
                total_cost_est = new_cost + pre_time_est
            if (1 - k) * delta_time <= total_cost_est <= (1 + k) * delta_time:
                priority = abs(delta_time - total_cost_est)
                new_path = copy.copy(current[3])
                new_path.append((next_cross, new_time))
                frontier.put((priority, next_cross, new_time, new_path, new_cost))
    return paths


def get_mfp(start_cross, end_cross, start_time):
    time_group = int(start_time.time().hour * 4 + start_time.time().minute / 15)
    weekday = 1 if start_time.weekday() < 5 else 0
    query = """SELECT id FROM visual_edge_odgroup 
            WHERE start_cross_id={s} AND end_cross_id={e}""".format(s=start_cross, e=end_cross)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        visual_edge_id = cursor.fetchone()[0]
        query = """SELECT path FROM main_path_odgroup WHERE edge_meta_id={meta_id} AND start_group_time<={tg} 
        AND end_group_time>={tg} AND weekday={weekday}""".format(meta_id=visual_edge_id, tg=time_group, weekday=weekday)
        cursor.execute(query)
        return cursor.fetchone()[0][1:-1].split(',')
    except Exception as e:
        traceback.print_exc(e)
    finally:
        cursor.close()
        conn.close()


def visual_to_map(paths, delta_time):
    # paths.sort(key=lambda x: abs(x[1] - delta_time))  # 按通行时长接近目标时长排序
    map_paths = []
    for path in paths:
        map_path = []
        for start_cross_time, end_cross_time in zip(path[0][:-1], path[0][1:]):
            mfp = get_mfp(start_cross_time[0], end_cross_time[0], start_cross_time[1])
            mfp_time = [(int(cross), start_cross_time[1]) for cross in mfp[:-1]]
            # mfp_time.append((end_cross_time[0], end_cross_time[1]))
            map_path.extend(mfp_time)
        map_path.append(path[0][-1])
        map_paths.append(map_path)
    return map_paths


def to_path_txt(map_paths, file_path):
    if not os.path.exists(os.path.dirname(file_path)):
        os.mkdir(os.path.dirname(file_path))
    result = ""
    i = 0
    for map_path in map_paths:
        for cross, cross_time in map_path:
            result += "{},{}\n".format(cross, cross_time)
        with open(file_path + '-{}.txt'.format(i), 'w') as f:
            f.write(result)
            i += 1


def road_score(map_paths):
    """路段数量"""
    map_len = [len(map_path) - 1 for map_path in map_paths]
    min_num, max_num = min(map_len), max(map_len)
    if min_num == max_num:
        return [1] * len(map_paths)
    score = []
    for map_num in map_len:
        score.append(np.e ** (0.5 * (min_num - map_num) / (max_num - min_num)))
    return score


def time_score(map_paths, delta_time):
    """通行时间"""
    map_time = [(map_path[-1][1] - map_path[1][1]).total_seconds() for map_path in map_paths]
    max_time, min_time = max(map_time), min(map_time)
    if max_time == min_time:
        return [1] * len(map_paths)
    score = []
    for cur_time in map_time:
        score.append(np.e ** (-2 * (abs(cur_time - delta_time)) / (max_time - min_time)))
    return score


def cos(road1, road2):
    l1 = np.sqrt(road1.dot(road1))
    l2 = np.sqrt(road2.dot(road2))
    return road1.dot(road2) / (l1 * l2)


def turn(road1, road2):
    """向量叉乘, >0顺时针, <0逆时针"""
    cos_angle = cos(road1, road2)
    if cos_angle < 0.866:  # 夹角大于30度认为是转向
        if road1[0] * road2[1] - road1[1] * road2[0] > 0:
            return -1  # 左转
        else:
            return 1  # 右转
    else:
        return 0  # 直行


def build_edge_vector(map_path):
    edge_vector = []
    gt_file = '/home/elvis/map/map-shp/Beijing2011/bj-road-epsg3785.gt'
    geo_map = RoadMap(gt_file)
    geo_map.load()
    for start_cross, end_cross in zip(map_path[:-1], map_path[1:]):
        start_pos, end_pos = geo_map.g.vertex_properties['pos'][start_cross[0]], geo_map.g.vertex_properties['pos'][
            end_cross[0]]
        edge_vector.append(np.array((end_pos[0] - start_pos[0], end_pos[1] - start_pos[1])))
    return edge_vector


def path_mode(map_path):
    edge_vector = build_edge_vector(map_path)
    edges_mode = []
    for pre_road, next_road in zip(edge_vector[:-1], edge_vector[1:]):
        edges_mode.append(turn(road1=pre_road, road2=next_road))
    return edges_mode


def mode_score(map_paths):
    scores = []
    for map_path in map_paths:
        sum_score = 0
        modes = path_mode(map_path)
        for mode in modes:
            if mode == 0:
                sum_score += 1
            elif mode == 1:
                sum_score += 0.9
            else:
                sum_score += 0.8
        scores.append(sum_score / (len(map_path) - 1))
    max_score = max(scores)
    min_score = min(scores)
    ret = []
    for score in scores:
        ret.append(np.e ** (-0.5 * (max_score - score) / (max_score - min_score)))
    return ret


def best_alternative(map_paths, delta_time):
    """计算最好的轨迹"""
    mtx = road_score(map_paths)
    mtx = np.column_stack((mtx, time_score(map_paths, delta_time), mode_score(map_paths)))
    criteria = [sk.MAX, sk.MAX, sk.MAX]
    data = sk.Data(mtx, criteria, cnames=['road', 'time', 'turn'])
    dm = closeness.TOPSIS()
    dec = dm.decide(data)
    return map_paths[dec.best_alternative_]


if __name__ == '__main__':
    time_s = time.time()
    # meta_id, start_index, count = 1, 0, 9
    meta_id, start_index, count = 539, 28, 50
    test_path = TestPath(meta_id, start_index, count)
    visual_map4000 = nx.read_gexf('/home/elvis/map/analize/analizeTime/countXEntTime/visualMapTop4000.gexf')
    k = 0.3  # k估计参数权值
    paths = []
    while len(paths) == 0:
        paths = find_paths(test_path.path_time[0], test_path.path_time[-1], visual_map4000, k)
        k *= 1.1
    delta_time = (test_path.path_time[-1][1] - test_path.path_time[0][1]).total_seconds()
    map_paths = visual_to_map(paths, delta_time)
    best_path = best_alternative(map_paths, delta_time)
    file_path = '/home/elvis/map/analize/path_restore/restore_path/{meta_id}/{meta_id}-{s}-{c}'.format(meta_id=meta_id,
                                                                                                       s=start_index,
                                                                                                       c=count)
    to_path_txt(map_paths, file_path)

    print('using time: {}'.format(time.time() - time_s))
