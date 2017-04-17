from pyspark import SparkContext
from pyspark.sql import SQLContext,Row

import os
os.environ['SPARK_CLASSPATH'] = "/home/ubuntu/postgresql-9.3-1103-jdbc41.jar"

sc = SparkContext(appName = "BuildProductRecommendations")
sqlContext = SQLContext(sc)

properties = {"user":"clientdemo", "password":"clientdemo4"}

url="jdbc:postgresql://postgresdemo.c3yxphqgag3s.ap-southeast-1.rds.amazonaws.com:5432/postgresdemo"

df = sqlContext.read.jdbc(url=url, table="trans", properties=properties)

print df.take(5)