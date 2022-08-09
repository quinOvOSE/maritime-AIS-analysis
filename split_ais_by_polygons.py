import geopandas as gpd
import mercantile
import random
import time
from shapely.geometry import Point, Polygon
from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType,StringType
from pyspark.sql.functions import col,to_date,unix_timestamp
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd

def get_polygon_from_csv(polygon_files_path):
    polygon_files = pd.read_csv(polygon_files_path)
    input_name = polygon_files['port_nameEn'].to_list()
    input_string = polygon_files['lat'].tolist()
    output_lat = []
    for i in input_string:
        temp = i[1:-1]
        output_lat.append([float(i) for i in temp.split(',')])
    input_string = polygon_files['lon'].tolist()
    output_lon = []
    for i in input_string:
        temp = i[1:-1]
        output_lon.append([float(i) for i in temp.split(',')])        
    polygon_ = list(map(lambda x,y:list(zip(x,y)),output_lon,output_lat)) 
    polygons_ = [Polygon(m) for m in polygon_]
    dict_ports = {}
    dict_ports['names'] = input_name
    dict_ports['geometry'] = polygons_
    gdf = gpd.GeoDataFrame(dict_ports, crs="EPSG:4326")
    return gdf
    

def big_udf(dict_ports):
    def perform_intersection(x, y):
        p = Point(x,y)
        index_ = dict_ports.loc[dict_ports.intersects(p)==True].index.tolist()[0]
        return dict_ports.loc[index_]['names']
    return F.udf(perform_intersection, T.StringType())


## INPUT
file_path = '/home/huangshichen/work/CTTIC/pos_jz_mmsi_list_2021'
polygon_files_path = '/home/huangshichen/work/CTTIC/data/port_polygon_info.csv'
dict_ports = get_polygon_from_csv(polygon_files_path)
## MAIN
#dict_ports = get_polygon_from_csv(polygon_files_path)
spark = SparkSession \
    .builder \
    .appName("Split by ports") \
    .getOrCreate()

df = spark.read.csv(file_path,header=True)
df = df.withColumnRenamed("航速(节)","航速")

df = df.withColumn("经度",df.经度.cast(DoubleType()))
df = df.withColumn("纬度",df.纬度.cast(DoubleType()))
df = df.withColumn("航向",df.航向.cast(DoubleType()))
df = df.withColumn("航速",df.航速.cast(DoubleType()))
df = df.withColumn("航艏向",df.航艏向.cast(DoubleType()))
df = df.withColumn('时间', 
                   to_date(unix_timestamp(col('时间'), 'yyyy-MM-dd HH:mm:ss').cast("timestamp")))

#%%time
df = df.withColumn("ports",big_udf(dict_ports)(F.col("经度"), F.col("纬度")))
df.filter(F.col("ports") != 'UNSET').write.csv('/home/huangshichen/work/CTTIC/ports_data_in_port_polygo0808')

#CPU times: user 3.89 s, sys: 1.46 s, total: 5.35 s
#Wall time: 18h 7min 11s
