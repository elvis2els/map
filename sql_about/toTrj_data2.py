#!/usr/bin/python3
# -*- coding: UTF-8 -*-
import argparse
import os
import time
from datetime import date

import pymysql
from progressbar import Percentage, Bar, ETA, FileTransferSpeed, ProgressBar

parser = argparse.ArgumentParser(description="using to insert data to mysql")
parser.add_argument('path', help="root path")
args = parser.parse_args()

widgets = ['Insert: ', Percentage(), ' ', Bar(marker='#'), ' ', ETA()]

def main():
    """
    数据插入表trj_data.
    :returns: nothing
    :raises: Exception
    """
    rootPath = args.path
    if not os.path.exists(rootPath):
        raise Exception("{} not exist!".format(rootPath))

    startTime = time.time()
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='369212',
                                 db='path_restore')
    
    metaRows = ()
    try:
        with connection.cursor() as cursor:
            sql = 'SELECT * FROM traj_metadata'
            cursor.execute(sql)
            metaRows = cursor.fetchall()
            print("获取metaRows时间:{}s".format(time.time()-startTime))
        
        insertSQL = 'insert into traj_data (metadata_id, cross_id, time)'
        insertSQL = insertSQL + 'values(%s,%s,%s);'
        trajNum = 0
        maxNum = 1000
        values = []
        pbar = ProgressBar(widgets=widgets, maxval=len(metaRows)).start()
        i = 0
        with connection.cursor() as cursor:
            for metaId, carId, subId, carDate in metaRows:
                filePath = os.path.join(rootPath, str(carId), carDate.strftime('%Y%m%d'), str(subId)+'.txt')
                with open(filePath, 'r') as f:
                    lines = f.readlines()
                    appends = [(str(metaId)+','+line.strip('\n')).split(',') for line in lines]
                    for value in appends:
                        values.append(value)
                trajNum += 1
                if trajNum == 1000:
                    cursor.executemany(insertSQL, values)
                    values = []
                    trajNum = 0
                i += 1
                pbar.update(i)
        connection.commit()

    finally:
        connection.close()

    

if __name__ == '__main__':
    main()
