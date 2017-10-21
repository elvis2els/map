#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import json
import time

import pymysql
from DBUtils.PooledDB import PooledDB
import pandas as pd
import os


class ODAnalize(object):
    def __init__(self):
        self.pool = PooledDB(pymysql, 5, host='192.168.3.199', user='root', passwd='123456', db='path_restore',
                             port=3306)
        self.od_list, self.max_od_group = self.read_od()

    def read_od(self):
        connection = self.pool.connection()
        cursor = connection.cursor()
        query = "SELECT max(od_group) FROM traj_metadata WHERE od_group is not null"
        cursor.execute(query)
        max_od_group = cursor.fetchone()[0]

        od_list = []  # 30%, 60%, 80%相似度, 节点存(ross集合, min(轨迹长度))
        base_od_group = 0
        s_time = time.time()
        while base_od_group <= max_od_group:
            query = "SELECT od_group, path FROM traj_metadata WHERE od_group>={} and od_group<={}".format(base_od_group,
                                                                                                          min(
                                                                                                              base_od_group + 3000,
                                                                                                              max_od_group))
            paths = pd.read_sql_query(query, connection)
            count_od = 0
            for od_group in range(base_od_group, min(base_od_group + 3000, max_od_group)):
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
                new_one_level_tag = True
                one_level_limit = 0.3
                two_level_limit = 0.6
                for one_level in od_list:
                    if self.is_similarity(new_od_list, one_level, similar_limit=one_level_limit):
                        new_two_level_tag = True     # 符合one_level_limit相似度标记
                        for two_level in one_level[2]:
                            find_groups_tag = True
                            if self.is_similarity(new_od_list, two_level, similar_limit=two_level_limit):
                                for od_group_list in one_level[2]:
                                    if self.group_similarity(od_group_list[0], new_od_list) < one_level_limit:
                                        new_two_level_tag = False
                                        break
                        if new_two_level_tag:
                            new_one_level_tag = False
                            one_level[2].append([new_od_list, od_group])
                            one_level[0] |= od_group_cross
                            one_level[1] = min(one_level[1], min_len)
                            break
                if new_one_level_tag:
                    od_list.append([od_group_cross, min_len, [[new_od_list, od_group]]])

                if count_od == 100:
                    count_od = 0
                    print("{}, {},{}".format(self.count_two_level(od_list), len(od_list), time.time() - s_time))

                # 测试用
                if od_group == 10000:
                    return od_list, od_group

            base_od_group = min(base_od_group + 3000, max_od_group)
        cursor.close()
        connection.close()
        return od_list, max_od_group

    # def read_od(self):
    #     connection = self.pool.connection()
    #     cursor = connection.cursor()
    #     query = "SELECT max(od_group) FROM traj_metadata WHERE od_group is not null"
    #     cursor.execute(query)
    #     max_od_group = cursor.fetchone()[0]
    #
    #     od_list = []  # 30%, 60%, 80%相似度, 节点存(ross集合, min(轨迹长度))
    #     base_od_group = 0
    #     s_time = time.time()
    #     while base_od_group <= max_od_group:
    #         query = "SELECT od_group, path FROM traj_metadata WHERE od_group>={} and od_group<={}".format(base_od_group,
    #                                                                                                       min(
    #                                                                                                           base_od_group + 3000,
    #                                                                                                           max_od_group))
    #         paths = pd.read_sql_query(query, connection)
    #         count_od = 0
    #         for od_group in range(base_od_group, min(base_od_group + 3000, max_od_group)):
    #             count_od += 1
    #             new_od_list = paths[paths.od_group == od_group]
    #             if len(new_od_list) == 0:
    #                 continue
    #             tmp_od_list = list(new_od_list.path)
    #             new_od_list = []
    #             for traj in tmp_od_list:
    #                 new_od_list.append(json.loads(traj))
    #             # 查找30分组
    #             od_group_cross = self.get_group_cross(new_od_list)
    #             min_len = self.min_len_group(new_od_list)
    #             new_30_tag = True
    #             for group_30 in od_list:
    #                 if self.is_similarity(new_od_list, group_30, similar_limit=0.5):
    #                     new_30_tag = False
    #                     new_60_tag = True
    #                     for group_60 in group_30[2]:
    #                         if self.is_similarity(new_od_list, group_60, similar_limit=0.6):
    #                             # 符合60%相似度, 插入60%节点子节点
    #                             new_60_tag = False
    #                             group_60[2].append([new_od_list, od_group])  # 子节点
    #                             group_60[0] |= od_group_cross  # 更新60%节点路口号集合
    #                             group_60[1] = min(group_60[1], min_len)  # 更新60%节点最小轨迹
    #                             break
    #                     group_30[0] |= od_group_cross
    #                     group_30[1] = min(group_30[1], min_len)
    #                     if new_60_tag:
    #                         group_30[2].append([od_group_cross, min_len, [[new_od_list, od_group]]])
    #                     break
    #             if new_30_tag:
    #                 od_list.append([od_group_cross, min_len, [[od_group_cross, min_len, [[new_od_list, od_group]]]]])
    #
    #             if count_od == 100:
    #                 count_od = 0
    #                 print("od: {} total time: {}".format(od_group, time.time() - s_time))
    #                 print("one level len: {}".format(len(od_list)))
    #                 print("two level len: {}".format(self.count_two_level(od_list)))
    #
    #             # 测试用
    #             if od_group == 10000:
    #                 return od_list, od_group
    #
    #         base_od_group = min(base_od_group + 3000, max_od_group)
    #     cursor.close()
    #     connection.close()
    #     return od_list, max_od_group

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
        new_30_tag = True
        group_num = -1
        for group_30 in self.od_list:
            if self.is_similarity(traj, group_30, similar_limit=0.5):
                new_30_tag = False
                new_60_tag = True
                for group_60 in group_30[2]:
                    if self.is_similarity(traj, group_60, similar_limit=0.6):
                        # 符合60%相似度
                        new_60_tag = False
                        new_80_tag = True
                        for group_80 in group_60[2]:
                            if self.is_same_group(traj, group_80[0]):
                                new_80_tag = False
                                group_80[0].append(traj)
                                group_num = group_80[1]
                                break  # 查找到符合80%的即停止
                        group_60[0] |= traj_cross  # 更新60%节点路口号集合
                        group_60[1] = min(group_60[1], traj_len)  # 更新60%节点最小轨迹
                        if new_80_tag:
                            group_60[2].append([[traj], self.max_od_group + 1])  # 子节点
                            self.max_od_group += 1
                            group_num = self.max_od_group
                        break
                group_30[0] |= traj_cross
                group_30[1] = min(group_30[1], traj_len)
                if new_60_tag:
                    group_30[2].append([traj_cross, traj_len, [[[traj], self.max_od_group + 1]]])
                    self.max_od_group += 1
                    group_num = self.max_od_group
                break
        if new_30_tag:
            self.od_list.append([traj_cross, traj_len, [[traj_cross, traj_len, [[[traj], self.max_od_group + 1]]]]])
            self.max_od_group += 1
            group_num = self.max_od_group
        return group_num

    @staticmethod
    def is_same_group(traj, od_group):
        for od_traj in od_group:
            if ODAnalize.similarity(traj, od_traj) < 0.8:
                return False
        return True

    def analize(self, start_index, end_index):
        for metaid, path in self.get_metaids(start_index, end_index):
            group_num = self.find_same_od_group(path)
            # self.od_group_to_sql(metaid, group_num)

    def get_metaids(self, start_index, end_index):
        connection = self.pool.connection()
        cursor = connection.cursor()
        query = "SELECT id, path FROM traj_metadata WHERE id>={} and id<={}".format(start_index, end_index)
        cursor.execute(query)
        meta_datas = cursor.fetchall()
        cursor.close()
        connection.close()
        for metaid, path in meta_datas:
            yield metaid, json.loads(path)

    def od_group_to_sql(self, metaid, group_num):
        connection = self.pool.connection()
        cursor = connection.cursor()
        update = "UPDATE traj_metadata SET od_group={od_group} WHERE id={metaid}".format(od_group=group_num,
                                                                                         metaid=metaid)
        cursor.execute(update)
        connection.commit()
        cursor.close()
        connection.close()

    def save_od_groups(self, file=None):
        if file is None:
            f = open("od_groups", mode='w')
        else:
            f = open(file, mode='w')
        f.write(json.dumps(self.od_list))
        f.close()


if __name__ == '__main__':
    time_s = time.time()
    od_analize = ODAnalize()
    # od_analize.save_od_groups()
    od_analize.analize(98157, 100000)
    print("total using time: {}".format(time.time() - time_s))
