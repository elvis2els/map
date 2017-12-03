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
    od_group_file = '/home/elvis/map/analize/analizeCross/od_group*entropy_v4.shp'
    # geo_df = geopandas.read_file(file)
    # pd_df = pd.DataFrame(geo_df)
    geo_df = geopandas.read_file(od_group_file)
    od_group_df = pd.DataFrame(geo_df)
    # od_group_df.score = od_group_df.score * pd_df.ENTROPY
    # od_group_df = od_group_df[od_group_df.score > 0]
    # od_group_df = od_group_df.sort(['score'], ascending=False)
    # od_group_df.reset_index()
    # od_group_df.rank = od_group_df.index
    type_id = get_type_id("count*od_group*entropy")
    df = pd.DataFrame({'cross_id': od_group_df['cross_id'], 'type': type_id, 'score': od_group_df['score']})
    df.to_sql('visual_cross', engine, if_exists='append', index=False, chunksize=100000)
