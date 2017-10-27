#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import json
import pickle
import time

import pandas as pd
import pymysql
from DBUtils.PooledDB import PooledDB


class ODAnalize(object):
    def __init__(self, od_group_file=None):
        self.pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore',
                             port=3306)
        self.one_level_limit = 0.15
        self.two_level_limit = 0.25
        self.od_list, self.max_od_group = self.read_od(od_group_file)


    def read_od(self, od_group_file):
        connection = self.pool.connection()
        cursor = connection.cursor()
        query = "SELECT max(od_group) FROM traj_metadata WHERE od_group is not null"
        cursor.execute(query)
        max_od_group = cursor.fetchone()[0]

        if od_group_file:
            f = open(od_group_file, 'rb')
            return pickle.load(f), max_od_group

        od_list = []  # 30%, 60%, 80%相似度, 节点存(ross集合, min(轨迹长度))
        base_od_group, interval = 0, 11000
        s_time = time.time()
        while base_od_group <= max_od_group + 1:
            query = "SELECT od_group, path FROM traj_metadata WHERE od_group>={} and od_group<={}".format(base_od_group,
                                                                                                          min(
                                                                                                              base_od_group + interval,
                                                                                                              max_od_group + 1))
            paths = pd.read_sql_query(query, connection)
            count_od = 0
            for od_group in range(base_od_group, min(base_od_group + 11000, max_od_group)):
                count_od += 1
                new_od_list = paths[paths.od_group == od_group]
                if len(new_od_list) == 0:
                    continue
                tmp_od_list = list(new_od_list.path)
                new_od_list = []
                for traj in tmp_od_list:
                    new_od_list.append(json.loads(traj))
                # 查找30分组
                od_group_cross = self.get_group_cross(new_od_list)
                min_len = self.min_len_group(new_od_list)
                find_same_group = False

                for one_level in od_list:
                    if self.is_similarity(new_od_list, one_level, similar_limit=self.one_level_limit):
                        in_one_level = True
                        for two_level in one_level[2]:  # 在第二层中查找
                            in_two_level = True
                            for old_od_group in two_level[2]:  # 比较叶子节点
                                similarity_score = self.group_similarity(old_od_group[0], new_od_list)
                                if similarity_score < self.one_level_limit:  # 不符合第一层相似度
                                    in_one_level = False
                                    break
                                if similarity_score < self.two_level_limit:
                                    in_two_level = False
                                    break
                            if not in_one_level:
                                break
                            if in_two_level:
                                find_same_group = True
                                node_three = [new_od_list, od_group]
                                two_level[2].append(node_three)
                                two_level[0] |= od_group_cross
                                two_level[1] = min(two_level[1], min_len)
                                break
                        if find_same_group:
                            one_level[0] |= od_group_cross
                            one_level[1] = min(one_level[1], min_len)
                            break
                        elif in_one_level:
                            find_same_group = True
                            node_three = [new_od_list, od_group]
                            node_two = [od_group_cross, min_len, [node_three]]
                            one_level[2].append(node_two)
                            one_level[0] |= od_group_cross
                            one_level[1] = min(one_level[1], min_len)
                            break
                if not find_same_group:
                    node_three = [new_od_list, od_group]
                    node_two = [od_group_cross, min_len, [node_three]]
                    node_one = [od_group_cross, min_len, [node_two]]
                    od_list.append(node_one)

                if count_od == 1000:
                    count_od = 0
                    # print("{},{},{},{}".format(od_group, self.count_two_level(od_list), len(od_list),
                    #                            time.time() - s_time))
                    print('{},{}'.format(od_group, time.time() - s_time))
                    self.save_od_groups()
                #
                # # 测试用
                # if od_group == 10000:
                #     return od_list, od_group

            base_od_group = min(base_od_group + interval, max_od_group + 1)
        cursor.close()
        connection.close()
        return od_list, max_od_group

    @staticmethod
    def count_two_level(od_list):
        count = 0
        for one_level in od_list:
            count += len(one_level[2])
        return count

    def get_group_cross(self, group):
        """获取组内包含的路口号集合"""
        ret = set()
        for traj in group:
            ret |= set(traj)
        return ret

    def min_len_group(self, group):
        """组内最小轨迹长度"""
        ret = len(group[0])
        for traj in group:
            ret = min(ret, len(traj))
        return ret

    def group_similarity(self, group1, group2):
        """计算平均相似性"""
        score, count = 0, len(group1) * len(group2)
        for traj1 in group1:
            for traj2 in group2:
                score += self.similarity(traj1, traj2)
        return score / count

    def is_similarity(self, group, node, similar_limit=0.3):
        min_num_cross = node[1] * similar_limit
        if isinstance(group[0], list):  # 若group为od组
            for traj in group:
                if not len(node[0] & set(traj)) >= min_num_cross:
                    return False
        elif not len(node[0] & set(group)) >= min_num_cross:  # 若group为traj
            return False
        return True

    @staticmethod
    def similarity(traj1, traj2):
        """两轨迹相似性计算"""
        straj1, straj2 = set(traj1), set(traj2)
        return float(len(straj1 & straj2)) / len(straj1 | straj2)

    def find_same_od_group(self, traj):
        """从od_list中查找同组od"""
        traj_cross = set(traj)
        traj_len = len(traj)
        find_same_group = False
        group_num = -1
        for one_level in self.od_list:
            if self.is_similarity(traj, one_level, similar_limit=self.one_level_limit):
                in_one_level = True
                for two_level in one_level[2]:  # 在第二层中查找
                    if self.is_similarity(traj, two_level, similar_limit=self.two_level_limit):
                        in_two_level = True
                        for old_od_group in two_level[2]:  # 比较叶子节点
                            find_same_group, similarity_score = self.is_same_group(traj, old_od_group)
                            if find_same_group:
                                old_od_group[0].append(traj)
                                group_num = old_od_group[1]
                                break
                            else:
                                if similarity_score < self.one_level_limit:  # 不符合第一层相似度
                                    in_one_level = False
                                    break
                                if similarity_score < self.two_level_limit:
                                    in_two_level = False
                                    break
                        if not in_one_level:
                            break
                        if find_same_group:
                            two_level[0] |= traj_cross
                            two_level[1] = min(two_level[1], traj_len)
                            break
                        elif in_two_level:
                            find_same_group = True
                            self.max_od_group += 1
                            group_num = self.max_od_group
                            node_three = [[traj], self.max_od_group]
                            two_level[2].append(node_three)
                            two_level[0] |= traj_cross
                            two_level[1] = min(two_level[1], traj_len)
                            break
                    else:
                        find_same_group, similarity_score = self.is_same_group(traj, two_level[2][0])
                        if similarity_score < self.one_level_limit:
                            in_one_level = False
                            break
                if find_same_group:
                    one_level[0] |= traj_cross
                    one_level[1] = min(one_level[1], traj_len)
                    break
                elif in_one_level:
                    find_same_group = True
                    self.max_od_group += 1
                    group_num = self.max_od_group
                    node_three = [[traj], self.max_od_group]
                    node_two = [traj_cross, traj_len, [node_three]]
                    one_level[2].append(node_two)
                    one_level[0] |= traj_cross
                    one_level[1] = min(one_level[1], traj_len)
                    break
        if not find_same_group:
            self.max_od_group += 1
            group_num = self.max_od_group
            node_three = [[traj], self.max_od_group]
            node_two = [traj_cross, traj_len, [node_three]]
            node_one = [traj_cross, traj_len, [node_two]]
            self.od_list.append(node_one)

        return group_num

    def is_same_group(self, traj, od_group):
        same_group_tag, min_similarity_score = True, 1
        for od_traj in od_group[0]:
            if min_similarity_score < self.one_level_limit:
                break
            similarity_score = ODAnalize.similarity(traj, od_traj)
            if similarity_score < 0.8:
                same_group_tag, min_similarity_score = False, min(min_similarity_score, similarity_score)
        return same_group_tag, min_similarity_score

    def analize(self, start_index, end_index):
        time_s, count = time.time(), 0
        ret = []
        for metaid, path in self.get_metaids(start_index, end_index):
            group_num = self.find_same_od_group(path)
            ret.append((metaid, group_num))
            if len(ret) == 1000:
                self.od_group_to_sql(ret)
                ret = []
                print('{},{}'.format(metaid, time.time()-time_s))
        if len(ret) > 0:
            self.od_group_to_sql(ret)

    def get_metaids(self, start_index, end_index):
        connection = self.pool.connection()
        cursor = connection.cursor()
        interval = 1000
        for start in range(start_index, end_index + 1, interval):
            end = start + interval
            if end_index - end < interval:
                end = end_index
            query = "SELECT id, path FROM traj_metadata WHERE id>={} and id<={}".format(start, end)
            cursor.execute(query)
            meta_datas = cursor.fetchall()
            for metaid, path in meta_datas:
                yield metaid, json.loads(path)
        cursor.close()
        connection.close()

    def od_group_to_sql(self, od_groups):
        connection = self.pool.connection()
        cursor = connection.cursor()
        for metaid, group_num in od_groups:
            update = "UPDATE traj_metadata SET od_group={od_group} WHERE id={metaid}".format(od_group=group_num,
                                                                                         metaid=metaid)
            cursor.execute(update)
        connection.commit()
        cursor.close()
        connection.close()

    def save_od_groups(self, file=None):
        if file is None:
            f = open("od_groups", mode='wb')
        else:
            f = open(file, mode='wb')
        pickle.dump(self.od_list, f)
        f.close()


if __name__ == '__main__':
    time_s = time.time()
    od_analize = ODAnalize('od_groups')
    # od_analize.save_od_groups()
    od_analize.analize(98157, 1218442+1)
    print("total using time: {}".format(time.time() - time_s))
