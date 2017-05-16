from DemoServerDB import ConnectDatabase
temp = ConnectDatabase()
query = 'SELECT * from master_table_history'

import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
from __future__ import division
import datetime as dt

temp.cur.execute(query)

col_names = [desc[0] for desc in temp.cur.description]
master_table_his = pd.DataFrame(temp.cur.fetchall(), columns=col_names)
list_of_indexes_new = [np.argmax(g['custom_date_identifier']) for l, g in master_table_his.groupby(['cust_id'])]
new_data = master_table_his.ix[list_of_indexes_new]
new_data.reset_index(drop=True, inplace=True)
new_data['custom_date_identifier'] = dt.datetime.now().date()
one_week = dt.datetime.now() - pd.Timedelta(days = 6)
week_data_index = np.where(master_table_his['custom_date_identifier'] <= one_week.date())
week_data = master_table_his.ix[week_data_index]
xyz = week_data.groupby(['cust_id', 'custom_date_identifier']).size().reset_index()

import datetime
def x(frame):
    if len(frame.custom_date_identifier) > 1:
        if frame.custom_date_identifier.iloc[-1] >= master_table_his['custom_date_identifier'].max():
            frame['time'] = frame.custom_date_identifier.iloc[-2]
        else:
            frame['time'] = frame.custom_date_identifier.iloc[-1]
    else:
        if not frame.custom_date_identifier.iloc[-1] >= master_table_his['custom_date_identifier'].max():
            frame['time'] = frame.custom_date_identifier.iloc[-1]
    return frame

dates = xyz.groupby('cust_id').apply(x)

del dates['custom_date_identifier']
dates = dates.drop_duplicates('cust_id')
dates.rename(columns={'time':'custom_date_identifier'}, inplace=True)
del dates[0]
old_data = pd.merge(week_data, dates, on=['cust_id', 'custom_date_identifier'], how='right')
old_data['custom_date_identifier'] = dt.datetime.now().date() - pd.Timedelta(days = 7)


cltv_value_old = old_data[['cltv', 'custom_date_identifier']]
cltv_old = cltv_value_old.groupby('custom_date_identifier').sum().reset_index()
cltv_value_new = new_data[['cltv', 'custom_date_identifier']]
cltv_new = cltv_value_new.groupby('custom_date_identifier').sum().reset_index()



value_cltv_old = old_data[['cltv', 'value', 'custom_date_identifier']]
value_old = value_cltv_old.groupby(['value','custom_date_identifier'])['cltv'].sum().reset_index()
value_old.rename(columns={'cltv':'cltv_sum'}, inplace=True)
value_cltv_new = new_data[['cltv', 'value', 'custom_date_identifier']]
value_new = value_cltv_new.groupby(['value','custom_date_identifier'])['cltv'].sum().reset_index()
value_new.rename(columns={'cltv':'cltv_sum'}, inplace=True)



churn_cust_old = old_data[['cust_id', 'churn', 'custom_date_identifier']]
churn_old = churn_cust_old.groupby(['churn','custom_date_identifier'])['cust_id'].count().reset_index()
churn_old.rename(columns={'cust_id':'customers'}, inplace=True)
churn_cust_new = new_data[['cust_id', 'churn', 'custom_date_identifier']]
churn_new = churn_cust_new.groupby(['churn','custom_date_identifier'])['cust_id'].count().reset_index()
churn_new.rename(columns={'cust_id':'customers'}, inplace=True)

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres_user:admin@paktortest.tuple-mia.com:5432/postgres_db')

churn_old.to_sql('churn_graph_previous', engine, if_exists='replace', index=False)
churn_new.to_sql('churn_graph_current', engine, if_exists='replace', index=False)

value_old.to_sql('value_graph_previous', engine, if_exists='replace', index=False)
value_new.to_sql('value_graph_current', engine, if_exists='replace', index=False)

cltv_old.to_sql('cltv_graph_previous', engine, if_exists='replace', index=False)
cltv_new.to_sql('cltv_graph_current', engine, if_exists='replace', index=False)



