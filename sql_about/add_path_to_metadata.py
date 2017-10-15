import pymysql
import json

if __name__ == '__main__':
    try:
        connection = pymysql.connect(host='192.168.3.237',
                                     user='root',
                                     password='369212',
                                     db='path_restore')
        with connection.cursor() as cursor:
            for metaid in range(1, 1218443):
                query = "SELECT cross_id FROM traj_data WHERE metadata_id={}".format(metaid)
                cursor.execute(query)
                crosses = cursor.fetchall()
                path = []
                for cross in crosses:
                    path.append(cross[0])
                update = "UPDATE traj_metadata SET path='{path}' WHERE id={metaid}".format(path=json.dumps(path), metaid=metaid)
                cursor.execute(update)
                connection.commit()
                if metaid % 1000 == 0:
                    print(metaid)
    except Exception as e:
        print(str(e))
