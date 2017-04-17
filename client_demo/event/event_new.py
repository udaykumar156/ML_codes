from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.mllib.recommendation import ALS
import pandas as pd
import datetime


print datetime.datetime.now()
print "RECOMMENDATION START"

sc = SparkContext(appName = "BuildProductRecommendations_event_new")
sqlContext = SQLContext(sc)
data = pd.read_csv('/home/ubuntu/client_demo/event_action_header', index_col=0)

n_prod_views_rdd = sqlContext.createDataFrame(data).rdd

del data

training_rdd, test_rdd = n_prod_views_rdd.randomSplit([8,2], 1345)

print datetime.datetime.now()

print "ALS MODEL"

model = ALS.trainImplicit(training_rdd, rank = 16, seed = 49247, iterations = 10, lambda_ = 0.1, alpha = 80.0)

print "ALS COMPLETE"

all_rec = model.recommendProductsForUsers(20).collect()

print "RECOMMENDATION COMPLETE"
