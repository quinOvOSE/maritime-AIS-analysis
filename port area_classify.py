import geopandas as gpd
import mercantile
import random
import time
from shapely.geometry import Point, Polygon
from pyspark.sql import Row
import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType,StringType, StructType, StructField,ArrayType
from pyspark.sql.functions import col,to_date,unix_timestamp
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
import pandas as pd
from sklearn.cluster import DBSCAN,KMeans
import numpy as np

file_path = '/home/.../Yantai'

df = spark.read.csv(file_path,header=False)

def dbscan_x(coords):
    kms_per_radian = 6371.0086
    epsilon = 0.2/kms_per_radian
    db = DBSCAN(eps = epsilon,min_samples = 20,
                algorithm='ball_tree',metric = 'haversine'
                ).fit(np.radians(coords))
    cluster_labels = db.labels_
    
    num_clusters = len(set(cluster_labels)-set([-1]))
    result = []
    coords_np = np.array(coords)
    kmeans = KMeans(n_clusters = 1,n_init=1,max_iter=10,random_state=100)
    for n in range(num_clusters):
        one_cluster = coords_np[cluster_labels==n]
        kk = kmeans.fit(one_cluster)
        center = kk.cluster_centers_
        latlng = center[0].tolist()
        result.append([latlng[1],latlng[0],n])
    return result


def dbscan_pandas_udf(data):
    data["cluster"] = DBSCAN(eps=5, min_samples=3).fit_predict(data[["_c1", "_c2"]])
    result = pd.DataFrame(data, columns=["_c1", "_c2" "cluster"])
    return result



output_schema = ArrayType(StructType(
            [
                StructField('_c1', DoubleType(),False),
                StructField('_c2', DoubleType(),False),
                StructField('clusterid', IntegerType(),False)
             ]
    ))



#df_stop = df.where(df._c4<=1)
df_stop = df
udf_dbscan = F.udf(lambda x: dbscan_x(x),output_schema)
#udf_dbscan = F.udf(lambda x: dbscan_x(x),IntegerType())

#data = df_stop.sample(0.3,123)
dataDF = df_stop.withColumn('point',F.array(df_stop._c1,df_stop._c2))\
.groupby('_c0').agg(F.collect_list('point').alias('point_list'))\
.withColumn('cluster',udf_dbscan(F.col('point_list')))

resultDF = dataDF.withColumn('centers',F.explode('cluster'))\
.select('_c0',F.col('centers').getItem('_c1').alias('_c1'),
       F.col('centers').getItem('_c2').alias('_c2'),
       F.col('centers').getItem('clusterid').alias('clusterid'))
       
resultDF.toPandas().to_csv('/home/.../test_yantai.csv')

