

# Pyspark script and scriptlets acting on the POP txt 
pop = spark.read.text("/Users/sid/dev/spark-lab-1/POP_PLACES_20160201.txt")
pop.show()

#
#	Lab Exercise 
#	Work out the latitude locations with the most towns or cities by 0.5 degree buckets
#

# first read in the text file as an RDD
popText = sc.textFile("/Users/sid/dev/spark-lab-1/POP_PLACES_20160201.txt")

# then skip the header line
header = popText.first()
popTextData = popText.filter(lambda line: line != header)
# popTextData.take(5)

# then split in to tokens
popTextDataSplit = popTextData.map(lambda line: line.split("|"))

# turn in to a dataframe
popDf = popTextDataSplit.toDF(header.split("|"))

# convert to a double
lats = popDf.selectExpr("cast(PRIM_LAT_DEC as double) latitude")

# create a new dataframe with the latitude rounded to the nearest 0.5
import pyspark.sql.functions
rlats = lats.select(col("latitude").alias("lat"), round(col("latitude").alias("rlat")*2.0)/2.0)

