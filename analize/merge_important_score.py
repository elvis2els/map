import geopandas
import pandas as pd


if __name__ == '__main__':
    file = '/home/elvis/map/analize/analizeCross/countXEnt_new.shp'
    od_group_file = '/home/elvis/map/analize/analizeCross/count_od_group.shp'
    geo_df = geopandas.read_file(file)
    pd_df = pd.DataFrame(geo_df)
    geo_df = geopandas.read_file(od_group_file)
    od_group_df = pd.DataFrame(geo_df)
    od_group_df.score = od_group_df.score * pd_df.ENTROPY
    od_group_df = od_group_df[od_group_df.score > 0]
    od_group_df = od_group_df.sort_values(by=['score'], ascending=False)
    od_group_df = od_group_df.reset_index(drop=True)
    od_group_df['rank'] = od_group_df.index

    path = '/home/elvis/map/analize/analizeCross/od_group*entropy_v4.shp'
    cross_scores = geopandas.GeoDataFrame(od_group_df, geometry='geometry')
    cross_scores.to_file(path)
