#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql
import time

connection = pymysql.connect(host='192.168.3.237',
                             user='root',
                             password='369212',
                             db='path_restore')


def get_metaids():
    with connection.cursor() as cursor:
        query = "SELECT id FROM traj_metadata"
        cursor.execute(query)
        metaids = cursor.fetchall()
    for metaid in metaids:
        yield metaid


def get_traj():
    for metaid in get_metaids():
        with connection.cursor() as cursor:
            query = "SELECT cross_id FROM traj_data WHERE metadata_id={}".format(metaid[0])
            cursor.execute(query)
            traj_detail = cursor.fetchall()
        traj = []
        for cross in traj_detail:
            traj.extend(cross)
        yield metaid[0], traj


def similarity(traj1, traj2):
    return float(len(set(traj1) & set(traj2))) / len(set(traj1) | set(traj2))


def is_same_group(traj, od_group):
    for od_traj in od_group:
        if similarity(traj, od_traj) < 0.8:
            return False
    return True


def analize_od():
    list_od = []
    for metaid, traj_detail in get_traj():
        time_s = time.time()
        print("metaid: {}".format(metaid))
        find_od_group = False
        for i, od_group in enumerate(list_od):
            if is_same_group(traj_detail, od_group):
                od_group.append(traj_detail)
                with connection.cursor() as cursor:
                    update_sql = "UPDATE traj_metadata SET od_group={od_group} WHERE id={metaid}".format(od_group=i,
                                                                                                         metaid=metaid)
                    cursor.execute(update_sql)
                    connection.commit()
                find_od_group = True
        if not find_od_group:
            with connection.cursor() as cursor:
                update_sql = "UPDATE traj_metadata SET od_group={od_group} WHERE id={metaid}".format(od_group=len(list_od),
                                                                                                 metaid=metaid)
                cursor.execute(update_sql)
                connection.commit()
            list_od.append([traj_detail])
        print("using time: {}".format(time.time() - time_s))


if __name__ == '__main__':
    time_s = time.time()
    analize_od()
    print("total using time: {}".format(time.time() - time_s))
