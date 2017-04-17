from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.mllib.recommendation import ALS,MatrixFactorizationModel, Rating
import datetime

print datetime.datetime.now()
print "RECOMMENDATION START"
sc = SparkContext(appName="EVENT_RECOMMENDATION")


data = sc.textFile('/home/ubuntu/client_demo/event/event_action_header')
data = data.zipWithIndex().filter(lambda tup: tup[1] > 0).map(lambda tup: tup[0])
event = data.map(lambda l: l.split(','))
ratings_rdd = event.map(lambda x: Rating(int(x[1]),int(x[2]), float(x[3])))
train_rdd, test_rdd = ratings_rdd.randomSplit([0.7,0.3],7856)

del data
del event
del ratings_rdd


print datetime.datetime.now()

print "ALS MODEL"
model = ALS.trainImplicit(train_rdd, rank = 16, seed = 49247, iterations = 10, lambda_ = 0.1, alpha = 80.0)

print "ALS COMPLETE"

all_rec = model.recommendProductsForUsers(20).collect()

print "RECOMMENDATION COMPLETE"
