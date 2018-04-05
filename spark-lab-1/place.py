

# Pyspark script and scriptlets acting on the POP txt 
pop = spark.read.text("/Users/sid/dev/pylab/spark-lab-1/GEO_PLACES_20160201.txt")
pop.show()

#
#	Lab Exercise 
#	Work out the latitude locations with the most towns or cities by 0.5 degree buckets
#

# first read in the text file as an RDD
popText = sc.textFile("/Users/sid/dev/pylab/spark-lab-1/GEO_PLACES_20160201.txt")

# then skip the header line
header = popText.first()
popTextData = popText.filter(lambda line: line != header)
# popTextData.take(5)

# then split in to tokens
popTextDataSplit = popTextData.map(lambda line: line.split("|"))

# turn in to a dataframe
popDf = popTextDataSplit.toDF(header.split("|"))
# check the data makes sense
# popDf.where(col("PRIM_LAT_DEC").like("-4%")).take(10)



# convert to a double
lats = popDf.selectExpr("cast(PRIM_LAT_DEC as double) latitude")

# create a new dataframe with the latitude rounded to the nearest 0.5
from pyspark.sql.functions import col
from pyspark.sql.functions import round
from pyspark.sql.functions import abs
#
# select:
#   latitude --> the latitude
#   rlat --> the latitude rounded to the nearest half degree
#   bucket --> a bucket representing half bucket intervals
#
rlats = lats.select(col("latitude").alias("lat"),(round(col("latitude")*2.0)/2.0).alias("rlat"),(abs((round(col("latitude")*2.0)/2.0)*2)).alias("bucket"))



# now group by the bucket and find the areas with the largest number of cities
from pyspark.sql.functions import count
from pyspark.sql.functions import lit
# rankedBuckets = rlats.groupBy(col("bucket")).agg(count(lit(1)).alias("num recs")).sort(col("num recs"), ascending=False)
rankedBuckets = rlats.groupBy(col("rlat"),col("bucket")).agg(count(lit(1)).alias("num recs")).sort(col("num recs"), ascending=False)
rankedBuckets.show()
# +----+------+--------+
# |rlat|bucket|num recs|
# +----+------+--------+
# |39.5|  79.0|   10161|
# |39.0|  78.0|    9355|
# |40.5|  81.0|    9248|
# |40.0|  80.0|    9208|
# |41.0|  82.0|    8085|
# |38.5|  77.0|    7538|
# |36.0|  72.0|    6967|
# |41.5|  83.0|    6879|
# |34.0|  68.0|    6637|
# |33.5|  67.0|    6587|
# |37.5|  75.0|    6457|
# |35.0|  70.0|    6391|
# |38.0|  76.0|    6102|
# |37.0|  74.0|    5766|
# |42.0|  84.0|    5748|
# | 0.0|   0.0|    5711|
# |36.5|  73.0|    5505|
# |35.5|  71.0|    5460|
# |34.5|  69.0|    5042|
# |42.5|  85.0|    4550|
# +----+------+--------+
# only showing top 20 rows