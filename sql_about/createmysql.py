#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb
import time
import os


def main():
    conn = MySQLdb.connect("127.0.0.1", "root", "369212hyl", "cabgps")
    cursor = conn.cursor()
    for root, days, filenames in os.walk('/dataPool/map/2015valid/'):
        for day in days:
            createSQL = "CREATE TABLE d%s (id INT NOT NULL AUTO_INCREMENT,time DATETIME NOT NULL,x DECIMAL(25,16) NOT NULL,y DECIMAL(24,16) NOT NULL,direction VARCHAR(10) NOT NULL,speed VARCHAR(10) NOT NULL,status CHAR(1) NOT NULL,cabid VARCHAR(32) NOT NULL, PRIMARY KEY (id));" % day
            # print createSQL
            cursor.execute(createSQL)
    conn.close()

if __name__ == '__main__':
    start = time.time()
    main()
    print time.time() - start
