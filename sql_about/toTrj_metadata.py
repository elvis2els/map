#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import os
import time

import pymysql
from progressbar import Percentage, Bar, ETA, FileTransferSpeed, ProgressBar

parser = argparse.ArgumentParser(description="using to insert data to mysql")
parser.add_argument('path', help="root path")
args = parser.parse_args()

widgets = ['Insert: ', Percentage(), ' ', Bar(marker='#'), ' ', ETA()]

def main():
    rootPath = args.path
    if not os.path.exists(rootPath):
        raise Exception("{} not exist!".format(rootPath))

    startTime = time.time()
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='369212',
                                 db='path_restore')

    try:
        with connection.cursor() as cursor:
            insertSQL = 'insert into traj_metadata (car_id, sub_id, date)'
            insertSQL = insertSQL + 'values(%s,%s,%s);'

            cabids = os.listdir(rootPath)
            values = []
            pbar = ProgressBar(widgets=widgets, maxval=len(cabids)).start()
            i = 0
            for cabid in cabids:
                cabidpath = os.path.join(rootPath, cabid)
                days = os.listdir(cabidpath)
                for day in days:
                    sub_ids = os.listdir(os.path.join(cabidpath, day))
                    for sub_id in sub_ids:
                        meta = [cabid, int(sub_id.strip('.txt')), day[
                                0:4] + '-' + day[4:6] + '-' + day[6:8]]
                        values.append(meta)
                i += 1
                pbar.update(i)
            cursor.executemany(insertSQL, values)
        connection.commit()
    finally:
        connection.close()


if __name__ == '__main__':
    start = time.time()
    main()
    print(time.time() - start)
