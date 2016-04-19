import os
import datetime
import psycopg2

config = {
    'user': 'postgres',
    'password': 'root',
    'host': 'localhost',
    'database': 'cabgps',
    'port': '5432'
}


def connection():
    return psycopg2.connect(**config)


def main():
    conn = connection()
    cur = conn.cursor()

    rootpath = "D:\\map\\2012.11北京出租车GPS数据\\cabGPS"

    cur.execute("SELECT cabid FROM d20121101 GROUP BY cabid;")
    rows = cur.fetchall()
    for row in rows:
        os.mkdir(rootpath + "\\" + row[0])


if __name__ == '__main__':
    start = datetime.datetime.now()
    main()
    end = datetime.datetime.now()
    print(end - start)
