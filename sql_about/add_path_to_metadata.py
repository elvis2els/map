import pymysql
import json
from DBUtils.PooledDB import PooledDB

if __name__ == '__main__':
    try:
        pool = PooledDB(pymysql, 5, host='192.168.3.237', user='root', passwd='369212', db='path_restore', port=3306)
        for metaid in range(172723, 1218443):
            connection = pool.connection()
            cursor = connection.cursor()
            query = "SELECT cross_id FROM traj_data WHERE metadata_id={}".format(metaid)
            cursor.execute(query)
            crosses = cursor.fetchall()
            path = []
            for cross in crosses:
                path.append(cross[0])
            update = "UPDATE traj_metadata SET path='{path}' WHERE id={metaid}".format(path=json.dumps(path), metaid=metaid)
            cursor.execute(update)
            connection.commit()
            cursor.close()
            connection.close()
            if metaid % 1000 == 0:
                print(metaid)
    except Exception as e:
        print(str(e))
