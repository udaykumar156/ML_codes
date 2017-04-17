
# coding: utf-8

# In[ ]:

from DemoServerDB import ConnectDatabase
from mapping import doc


# In[ ]:

temp = ConnectDatabase()


# In[ ]:

temp.conn


# In[ ]:

doc['column_map']['TRANSACTION_MASTER']


# In[ ]:

query = 'SELECT "%s","%s","%s" FROM "%s"'%(doc['column_map']['TRANSACTION_MASTER']['cust_id'],
                                           doc['column_map']['TRANSACTION_MASTER']['product_id'],
                                           doc['column_map']['TRANSACTION_MASTER']['timestamp'],
                                           doc['table_map']['TRANSACTION_MASTER'])


# In[ ]:

print query


# In[ ]:

temp.conn.rollback()


# In[ ]:

temp.cur.execute(query)


# In[ ]:

col_names = [desc[0] for desc in temp.cur.description]


# In[ ]:

col_names


# In[ ]:

import pandas as pd
import numpy as np
import datetime as dt


# In[ ]:

trans_table = pd.DataFrame(temp.cur.fetchall(), columns=col_names)


# In[ ]:

trans_table.head()


# In[ ]:

trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['cust_id']:'cust_id'},inplace=True)
trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['product_id']:'product_id'},inplace=True)
trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['timestamp']:'timestamp'},inplace=True)


# In[ ]:

print trans_table.head()


# In[ ]:

trans_table['timestamp'] = pd.to_datetime(trans_table['timestamp'])


# In[ ]:

trans_table.shape


# In[ ]:

#len(trans_table.cust_id.unique())


# In[ ]:

#len(trans_table.product_id.unique())


# In[ ]:

group_data = trans_table.groupby(['cust_id', 'product_id']).size().reset_index().rename(columns={0:'counter'})


# In[ ]:

#group_data.head()


# In[ ]:

#group_data.shape


# In[ ]:

repeated_data = group_data.query('counter != 1')


# In[ ]:

#repeated_data.head()


# In[ ]:

#len(repeated_data.cust_id.unique())


# In[ ]:

#len(repeated_data.product_id.unique())


# In[ ]:

#repeated_data.head()


# In[ ]:

#trans_table.head()


# In[ ]:

xyz = pd.merge(trans_table, repeated_data, on=['cust_id', 'product_id'], how='left')


# In[ ]:

#xyz.head()


# In[ ]:

#xyz.shape


# In[ ]:

#len(xyz.cust_id.unique())


# In[ ]:

#len(xyz.product_id.unique())


print "time"

def x(frame):
    frame['time'] = frame['timestamp'].max() - frame['timestamp'].min()
    return frame


# In[ ]:

dates = xyz.groupby(['cust_id','product_id'], group_keys=False).apply(x)


# In[ ]:

print dates.head()


# In[ ]:

dates.shape


# In[ ]:

len(dates.cust_id.unique())


# In[ ]:

len(dates.product_id.unique())


# In[ ]:

dates.reset_index(drop=True,inplace=True)


# In[ ]:

dates.head()


# In[ ]:

cust_repur = dates


# In[ ]:

cust_repur.head()


# In[ ]:

cust_repur.shape


# In[ ]:

cust_repur.reset_index(drop=True, inplace=True)


# In[ ]:

cust_repur.head()


# In[ ]:

cust_repur.shape


# In[ ]:

list_of_indexes = [np.argmax(g['timestamp']) for l, g in cust_repur.groupby(['cust_id','product_id'])]


# In[ ]:

cust_repur_original = cust_repur.ix[list_of_indexes]


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original.reset_index(drop=True,inplace=True)


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['days_int'] = cust_repur_original['time'].dt.days


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['repurchase_time'] = (cust_repur_original['days_int']-1) / (cust_repur_original['counter']-1)


# In[ ]:

cust_repur_original.head()


# In[ ]:

len(cust_repur_original.product_id.unique())


# In[ ]:

abc = pd.pivot_table(cust_repur_original, index=['cust_id'], columns=['product_id'], values=['repurchase_time'], aggfunc='first')


# In[ ]:

abc.shape


# In[ ]:

abc.head()


# In[ ]:

type(abc)


# In[ ]:

mi = abc.columns


# In[ ]:

mii = mi.tolist()


# In[ ]:

ind = pd.Index([e[1] for e in mii])


# In[ ]:

abc.columns = ind


# In[ ]:

abc.head()


# In[ ]:

products = pd.DataFrame(abc.columns)


# In[ ]:

products.rename(columns={0:'product_id'}, inplace=True)


# In[ ]:

art = abc[products.product_id[products.index]].sum() / abc[products.product_id[products.index]].count()


# In[ ]:

type(art)


# In[ ]:

art = art.to_frame()


# In[ ]:

art.reset_index(inplace=True)


# In[ ]:

art.rename(columns={'index':'product_id'}, inplace=True)


# In[ ]:

art.rename(columns={0:'average_repurchase_time'}, inplace=True)


# In[ ]:

art.head()


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['time_diff'] = dt.date.today() - cust_repur_original["timestamp"].dt.date


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original.tail()


# In[ ]:

cust_repur_original.counter = cust_repur_original.counter.fillna(0)


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['time_diff_int'] = cust_repur_original['time_diff'].dt.days


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original_final = cust_repur_original.merge(art, on=['product_id'], how='left')


# In[ ]:

cust_repur_original_final.head()


# In[ ]:

cust_repur_original_final.repurchase_time.fillna(cust_repur_original_final.average_repurchase_time, inplace=True)


# In[ ]:

cust_repur_original_final.head()


# In[ ]:

cust_repur_original_final['depriotarize'] = np.where(cust_repur_original_final['time_diff_int'] > cust_repur_original_final['repurchase_time'], 'Recommend', 'Do_not_recommend')


# In[ ]:

cust_repur_original_final.head()


# In[ ]:

del cust_repur_original_final['timestamp']


# In[ ]:

del cust_repur_original_final['counter']


# In[ ]:

del cust_repur_original_final['time']


# In[ ]:

del cust_repur_original_final['days_int']


# In[ ]:

del cust_repur_original_final['repurchase_time']


# In[ ]:

del cust_repur_original_final['time_diff']


# In[ ]:

del cust_repur_original_final['time_diff_int']


# In[ ]:

del cust_repur_original_final['average_repurchase_time']


# In[ ]:

cust_repur_original_final.head()


# In[ ]:

len(cust_repur_original_final.cust_id.unique())


# In[ ]:

len(cust_repur_original_final.product_id.unique())


# In[ ]:




# In[ ]:



