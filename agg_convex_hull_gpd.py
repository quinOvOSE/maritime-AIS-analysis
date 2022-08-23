import geopandas as gpd
import glob
import pandas as pd
import tqdm


def Load_csv_from_path(path):
    files = glob.glob(path+'*.csv')    
    df = pd.DataFrame()
    for file in tqdm.tqdm(files):
        file_pd = pd.read_csv(file,header=0)
        file_pd['path'] = file
        file_pd['clusterid_new'] = file_pd['path']+file_pd['clusterid'].astype(str) 
        df = pd.concat([df,file_pd],axis=0)
    return df
    
path = '/home/'

data = Load_csv_from_path(path)

data = data[data['clusterid']>-1]
data_lst =  list(data.groupby('clusterid_new'))
polygons = gpd.GeoDataFrame()
for cluster in tqdm.tqdm(data_lst):
    points = cluster[1]
    points_gpd = gpd.GeoDataFrame(points, geometry=gpd.points_from_xy(points['_c1'],
                                                                      points['_c2']))
    polygon = points_gpd.unary_union.convex_hull
    polygon_ = gpd.GeoDataFrame(geometry = [polygon])
    polygons = pd.concat([polygons,polygon_])
    
polygons.to_file('/home/huangshichen/work/test_7.5_5.gpkg')    
