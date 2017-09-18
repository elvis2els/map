import pymysql
import time

from Config import Config
from path_restore import EstTime


def get_visual_edge():
    config = Config()
    database_conf = config.getConf('database')
    connection = pymysql.connect(
            database_conf['host'],
            database_conf['user'],
            database_conf['passwd'],
            database_conf['name'])
    with connection.cursor() as cursor:
        query = 'SELECT start_cross_id, end_cross_id FROM visual_edge'
        cursor.execute(query)
        edges = cursor.fetchall()
        return edges

def main():
    edges = get_visual_edge()
    est = EstTime()
    time_s, i, maxlen = time.time(), 6235, len(edges)
    for edge in edges:
        if time.time() - time_s > 1:
            print('{}/{}'.format(i, maxlen), end='\r')
            time_s = time.time()
        i += 1
        est.est_cross_time(edge[0], edge[1])

if __name__ == '__main__':
    time_s = time.time()
    main()
    print('using time: {}'.format(time.time() - time_s))
    print('done')
