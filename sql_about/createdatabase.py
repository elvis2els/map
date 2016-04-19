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


def main():
    conn = connection()
    cur = conn.cursor()

    rootpath = "D:\\map\\2012.11 北京出租车GPS数据\\epsg3785"
    for root, dirs, filenames in os.walk(rootpath):
        i = 1
        for day in dirs:
            d_table = "d" + day
            d_index = "index_cabid" + str(i)
            createSQL = "create table %s(id serial primary key, cabid varchar(6) not null, event varchar(1) not null, status varchar(1) not null, time time not null, x decimal not null, y decimal not null, speed smallint not null, direction smallint not null, gps_status varchar(1) not null);" % d_table
            cur.execute(createSQL)
            indexSQL = "create index %s on %s (cabid);" % (d_index, d_table)
            cur.execute(indexSQL)
            i = i + 1
    conn.commit()

if __name__ == '__main__':
    start = time.time()
    main()
    end = time.time()
    print(end - start)
