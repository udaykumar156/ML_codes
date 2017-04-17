
# coding: utf-8

import psycopg2
import pandas as pd
import findspark
import math
from pyspark import SparkContext
from pyspark.sql import SQLContext,Row
from pyspark.mllib.recommendation import ALS

conn = psycopg2.connect(
                database = 'postgres_db',
                user = 'postgres_user',
                password = 'admin',
                host = 'client.tuple-mia.com',
                port = '5432',
                sslmode = 'require'
            )

print "CONNECTION_ESTABLISHED"

temp = conn.cursor()

import yaml
with open('/home/ubuntu/MappingBuffer_event.yml', 'r') as f:
    doc = yaml.load(f)


query = "SELECT * FROM %s"%(doc['table_map']['EVENT_LOG'])

# print query

temp.execute(query)

event = temp.fetchall()

colnames = [desc[0] for desc in temp.description]

event_df = pd.DataFrame(event, columns=colnames)

event_df.rename(columns={doc['column_map']['EVENT_LOG']['cust_id']:'cust_id'}, inplace=True)
event_df.rename(columns={doc['column_map']['EVENT_LOG']['product_id']:'product_id'}, inplace=True)
event_df.rename(columns={doc['column_map']['EVENT_LOG']['action_type']:'actiontype'}, inplace=True)

event_data = event_df[['cust_id', 'product_id', 'actiontype']]

# type(event_data)
# type(event_df)

#event_data.shape, event_df.shape

del event_df
del event

#event_data.head()

event_data.actiontype.replace([3],[-1], inplace=True)

findspark.init()

sc = SparkContext(appName = "Recommendation_event")

sqlContext = SQLContext(sc)

print "Creating RDD"

n_prod_views_rdd = sqlContext.createDataFrame(event_data).rdd

training_rdd, test_rdd = n_prod_views_rdd.randomSplit([8,2], 1345)

print "ALS MODEl"

model = ALS.trainImplicit(n_prod_views_rdd, rank=16, seed=49247, iterations=10, lambda_=0.1, alpha=80.0)

print "ALS COMPLETE"
user_rec = model.recommendProductsForUsers(20).collect()

user_rec1 = []

for x in range(len(user_rec)):
    for y in range(20):
        v = user_rec[x][1][y][0:3]
        d = list(v)
        user_rec1.append(d)

del user_rec

print "Recommendation DF"

user_rec_df = pd.DataFrame(user_rec1, columns=['cust_id', 'recommend_prod', 'rating'])

del user_rec1

from sqlalchemy import create_engine

engine = create_engine('postgresql://postgres_user:admin@client.tuple-mia.com:5432/postgres_db')

user_rec_df.to_sql('event_recommendations', engine)

print "Table written to postgres"
