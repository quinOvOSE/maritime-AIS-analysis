# maritime-AIS-analysis

# TODO ports detect
1. build buffer on costlines (0.001 degree ,about 111 meters)
2. clip points which in buffer of costlines
3. cluster method on cliped points?
4. analysis the distrion on distance between clusters and known ports. find a threahold

(if the numbers of points or fileshape of polygon is sample ,use Qis with function of Join attributes by nearest)

# split_ais_by_polygons
we used pyspark with geopandas to split ais data by polygons such like port boundaries or something.
geopandas mount in pyspark by UDF (user define function). specially a polygons pass into the pyspark as a constarint files. so we use udf in udf to make it.
