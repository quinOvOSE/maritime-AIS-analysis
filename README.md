# maritime-AIS-analysis
# split_ais_by_polygons
we used pyspark with geopandas to split ais data by polygons such like port boundaries or something.
geopandas mount in pyspark by UDF (user define function). specially a polygons pass into the pyspark as a constarint files. so we use udf in udf to make it.
