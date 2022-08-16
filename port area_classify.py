from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType,BooleanType,DateType,DoubleType,StringType, StructType, StructField,ArrayType
from pyspark.sql.functions import col,lit
from pyspark.sql.session import SparkSession
import pandas as pd
from sklearn.cluster import DBSCAN,KMeans,OPTICS
import numpy as np


def dbscan_x1(coords):
    kms_per_radian = 6371.0086
    epsilon = 0.2/kms_per_radian
    db = OPTICS(eps = epsilon,min_samples = 20,
                algorithm='ball_tree',metric = 'haversine'
                ).fit(np.radians(coords))
    cluster_labels = db.labels_.reshape(-1,1)
    coords_np = np.array(coords)
    return np.concatenate([coords_np,cluster_labels],axis=1).tolist()

def dbscan_x2(coords):
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

output_schema = ArrayType(StructType(
            [
                StructField('_c1', DoubleType(),False),
                StructField('_c2', DoubleType(),False),
                StructField('clusterid', DoubleType(),False)
             ]
    ))
    
    
# INPUT
file_path = '/home/'

save_path = '/home/'
output_schema = ArrayType(StructType(
            [
                StructField('_c1', DoubleType(),False),
                StructField('_c2', DoubleType(),False),
                StructField('clusterid', DoubleType(),False)
             ]
    ))

GROUPBY = True

# READ FILE AND PROCESSING
spark = SparkSession \
    .builder \
    .appName("DBSCAN") \
    .getOrCreate()
df = spark.read.csv(file_path,header=False)
df = df.withColumn("_c1",df._c1.cast(DoubleType()))
df = df.withColumn("_c2",df._c2.cast(DoubleType()))




# SPLIT AREA
df_stop = df.filter('_c1 > 117.2202 and _c1 < 119.9410 and _c2 > 37.4277 and _c2 < 40.1261')
df_stop = df_stop.sample(0.1)
df_stop.count()

if GROUPBY:
    
    df_stop = df_stop.withColumn('group',lit(1))
    udf_dbscan = F.udf(lambda x: dbscan_x1(x),output_schema)


    dataDF = df_stop.withColumn('point',F.array(df_stop._c1,df_stop._c2))\
    .groupby('group').agg(F.collect_list('point').alias('point_list'))\
    .withColumn('cluster',udf_dbscan(F.col('point_list')))

    resultDF = dataDF.withColumn('centers',F.explode('cluster'))\
    .select('group',F.col('centers').getItem('_c1').alias('_c1'),
           F.col('centers').getItem('_c2').alias('_c2'),
           F.col('centers').getItem('clusterid').alias('clusterid'))

aa = resultDF.toPandas()

