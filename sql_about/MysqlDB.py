#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import pymysql
from DBUtils.PooledDB import PooledDB
from contextlib import contextmanager


class MysqlDB(object):
    def __init__(self, max_num, host, user, passwd, db, port=3306):
        self.pool = PooledDB(pymysql, max_num, host=host, user=user, passwd=passwd, db=db, port=port)

    @contextmanager
    def cursor(self):
        try:
            connection = self.pool.connection()
            cursor = connection.cursor()
            yield cursor
            connection.commit()
        except Exception as e:
            print(str(e))
            connection.rollback()
        finally:
            cursor.close()
            connection.close()

    def connection(self):
        return self.pool.connection()
