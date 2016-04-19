import psycopg2
import time
import os

config = {
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'database': 'cabgps',
    'port': '5432'
}


def connection():
    return psycopg2.connect(**config)


def buildlist(sourcepath_name, i):
    with open(sourcepath_name, "r") as f:
        line = f.readline()
        columns_1day = []
        columns_2day = []
        while True:
            if not line:
                f.close()
                return columns_1day, columns_2day
            column = []
            line = line.split(',')
            column = line[:]
            column[8] = column[8][0:1]
            column[3] = column[3][-6:]
            if int(line[3][6:8]) == i:
                columns_1day.append(column)
            elif int(line[3][6:8]) == i + 1:
                columns_2day.append(column)
            line = f.readline()


def insertDate(conn, columns_1day, columns_2day, day):
    cur = conn.cursor()
    d_table1 = "d" + day
    d_table2 = "d" + str(int(day) + 1)
    insert1 = "INSERT INTO %s (cabid, event, status, time, x, y, speed, direction, gps_status)" % d_table1
    insert2 = "INSERT INTO %s (cabid, event, status, time, x, y, speed, direction, gps_status)" % d_table2
    insertSQL = insert1 + " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    insertSQL2 = insert2 + " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    if columns_1day:
        cur.executemany(insertSQL, columns_1day)
    if columns_2day:
        cur.executemany(insertSQL2, columns_2day)
    conn.commit()


def main():
    try:
        conn = connection()
        print("connect success")
    except Exception as e:
        print("connect error!")
        raise e

    day = "20121101"
    while int(day) <= 20121130:
        i = int(day[-2:])
        rootpath = "D:\\map\\2012.11 北京出租车GPS数据\\epsg3785\\" + day
        for root, dirs, filenames in os.walk(rootpath):
            for filename in filenames:
                columns_1day, columns_2day = buildlist(
                    rootpath + '\\' + filename, i)
                insertDate(conn, columns_1day, columns_2day, day)
                print("commplete:" + filename)
        day = str(int(day) + 1)
    conn.close()


if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)
