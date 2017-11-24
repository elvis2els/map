import geopandas
import pandas as pd
import pymysql
from sqlalchemy import create_engine

db = pymysql.connect(host='192.168.3.199', user='root', passwd='123456', db='path_restore')
engine = create_engine("mysql+pymysql://root:123456@192.168.3.199:3306/path_restore")


def get_type_id(visual_type):
    """获取type对应id"""
    with db.cursor() as cursor:
        query = "SELECT id FROM visual_type WHERE type_name = '{}'".format(visual_type)
        cursor.execute(query)
        type_id = cursor.fetchone()[0]
        return type_id


if __name__ == '__main__':
    file = '/home/elvis/map/analize/analizeCross/countXEnt_new.shp'
    geo_df = geopandas.read_file(file)
    pd_df = pd.DataFrame(geo_df)
    type_id = get_type_id("count*entropy")
    df = pd.DataFrame({'cross_id': pd_df['INDEX'], 'type': type_id, 'score': pd_df['COUNT']*pd_df['ENTROPY']})
    df.to_sql('visual_cross', engine, if_exists='append', index=False, chunksize=100000)
