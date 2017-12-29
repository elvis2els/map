import time
import traceback
import copy

import networkx as nx
import pymysql
from DBUtils.PooledDB import PooledDB
from queue import PriorityQueue

from path_restore.path_restore import EstTime

pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore', port=3306)


class TestPath(object):
    def __init__(self, car_id, sub_id, date, start_index, count):
        self.car_id = car_id
        self.sub_id = sub_id
        self.date = date
        self.path_time = self.get_path(start_index, count)

    def get_path(self, start_index, count):
        query = """
        SELECT id
        FROM traj_metadata
        WHERE car_id = {car_id} AND sub_id = {sub_id} AND date = '{date}'
        """.format(car_id=self.car_id, sub_id=self.sub_id, date=self.date)
        conn = pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            meta_id = cursor.fetchone()[0]
            query = """SELECT cross_id, time FROM traj_data 
                WHERE metadata_id={meta_id} LIMIT {start}, {count}""".format(meta_id=meta_id, start=start_index,
                                                                             count=count)
            cursor.execute(query)
            return cursor.fetchall()
        except Exception as e:
            traceback.print_exc(e)
        finally:
            cursor.close()
            conn.close()


def get_pre_time(start_cross, end_cross, start_cross_time):
    start_time = start_cross_time.time()
    time_group = int(start_time.hour * 4 + start_time.minute / 15)
    weekday = 1 if start_cross[1].weekday() < 5 else 0
    query = """SELECT cost
            FROM cross_pre_time_all
            WHERE start_cross_id = {s} AND end_cross_id = {e} AND time_group = {time_group} AND weekday = {weekday}
            """.format(s=start_cross, e=end_cross, time_group=time_group, weekday=weekday)
    conn = pool.connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        cost = cursor.fetchone()
        if cost is not None and len(cost) > 0:
            return cost[0]
        else:
            est = EstTime()
            est.est_cross_time(start_cross[0], end_cross[0])
            cursor.execute(query)
            return cursor.fetchone[0]
    except Exception as e:
        traceback.print_exc(e)
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
        traceback.print_exc(e)
    finally:
        cursor.close()
        conn.close()


def find_paths(start_cross, end_cross, visual_map):
    delta_time = (end_cross[1] - start_cross[0]).total_seconds()
    paths = []
    frontier = PriorityQueue()
    frontier.put((delta_time, start_cross[0], start_cross[1], [start_cross], 0))  # 分别代表与目标时间差值,节点号,时间,路径,实际消耗时间

    while not frontier.empty() or len(paths) < 10:
        current = frontier.get()
        if current[1] == end_cross[0]:
            paths.append([current[3], current[4]])
            continue
        current_time = current[2]
        current_time_group = int(current_time.time().hour * 4 + current_time.time().minute / 15)
        weekday = 1 if current_time.weekday() < 5 else 0
        for next_cross in visual_map.neighbors_iter(str(float(current))):   # 计算每个出边
            next_cross = int(float(next_cross))
            if next_cross in current[3]:
                continue
            c_to_n_cost = get_cost(current[1], next_cross, current_time_group, weekday)
            new_cost = current_time[4] + c_to_n_cost
            total_cost_est = new_cost + get_pre_time(current[1], int(float(next_cross)), current_time)
            if (1 - 0.3) * delta_time <= total_cost_est <= (1 + 0.3) * delta_time:
                priority = abs(delta_time - total_cost_est)
                new_time = current_time + c_to_n_cost
                new_path = copy.copy(current[3])
                new_path.append(next_cross)
                frontier.put((priority, next_cross, new_time, new_path, new_cost))
    return paths


if __name__ == '__main__':
    time_s = time.time()
    test_path = TestPath(1310, 19, '2015-11-18', 2, 6)
    visual_map4000 = nx.read_gexf('/home/elvis/map/analize/analizeTime/countXEntTime/visualMapTop4000.gexf')
    find_paths(test_path.path_time[0], test_path.path_time[-1], visual_map4000)
    print('using time: {}'.format(time_s - time.time()))
