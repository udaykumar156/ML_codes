
# In[2]:

from DemoServerDB import ConnectDatabase
from mapping import doc


# In[3]:

temp = ConnectDatabase()


# In[4]:

print temp.conn


# In[5]:

doc['column_map']['TRANSACTION_MASTER']


# In[6]:

query = 'SELECT "%s","%s","%s" FROM "%s"'%(doc['column_map']['TRANSACTION_MASTER']['cust_id'],
                                           doc['column_map']['TRANSACTION_MASTER']['category'],
                                           doc['column_map']['TRANSACTION_MASTER']['timestamp'],
                                           doc['table_map']['TRANSACTION_MASTER'])


# In[7]:

print query


# In[8]:

temp.conn.rollback()


# In[9]:

temp.cur.execute(query)


# In[10]:

col_names = [desc[0] for desc in temp.cur.description]


# In[11]:

col_names


# In[12]:

import pandas as pd
import numpy as np


# In[13]:

trans_table = pd.DataFrame(temp.cur.fetchall(), columns=col_names)


# In[14]:

print trans_table.head()


# In[15]:

trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['cust_id']:'cust_id'},inplace=True)
trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['category']:'category'},inplace=True)
trans_table.rename(columns={doc['column_map']['TRANSACTION_MASTER']['timestamp']:'timestamp'},inplace=True)


# In[16]:

trans_table.head()


# In[17]:

trans_table['timestamp'] = pd.to_datetime(trans_table['timestamp'])


# In[18]:

#type(trans_table.timestamp[0])


# In[19]:

#trans_table.shape


# In[20]:

print len(trans_table.cust_id.unique())


# In[21]:

print len(trans_table.category.unique())


# In[22]:

cust_category = trans_table[['cust_id', 'category']]


# In[23]:

group_data = trans_table.groupby(['cust_id', 'category']).size().reset_index().rename(columns={0:'counter'})


# In[24]:

group_data.head()


# In[25]:

group_data.shape


# In[26]:

repeated_data = group_data.query('counter != 1')


# In[27]:

repeated_data.head()


# In[28]:

#len(repeated_data.cust_id.unique())


# In[29]:

#len(repeated_data.category.unique())


# In[30]:

#repeated_data.head()


# In[31]:

#trans_table.head()


# In[32]:

xyz = pd.merge(trans_table, repeated_data, on=['cust_id', 'category'], how='left')


# In[33]:

xyz.head()


# In[34]:

xyz.shape


# In[35]:

len(xyz.cust_id.unique())


# In[36]:

len(xyz.category.unique())


# In[ ]:

def x(frame):
    frame.sort_values('timestamp', inplace=True)
    frame['time'] = frame['timestamp'] - frame['timestamp'].shift(1)
    return frame


# In[ ]:

dates = xyz.groupby(['cust_id','category'], group_keys=False).apply(x)


print "total days is calculated"
# In[ ]:

dates.head()


# In[ ]:

dates.shape


# In[ ]:

dates.reset_index(drop=True,inplace=True)


# In[ ]:

dates.head()


# In[ ]:

dates.time = dates.time.fillna(0)


# In[ ]:

dates.head()


# In[ ]:

def y(frame):
    frame['sum_days'] = frame['time'].sum()
    return frame


# In[ ]:

dates_sum = dates.groupby(['cust_id','category']).apply(y)


print "sum of days is calculated"

# In[ ]:

#dates_sum.head()


# In[ ]:

#dates_sum.tail()


# In[ ]:

dates_sum['days_int'] = ""


# In[ ]:

#dates_sum.head()


# In[ ]:

for x in range(0,len(dates_sum)):
    dates_sum.days_int[x] = dates_sum.sum_days[x].days


# In[ ]:

#dates_sum.head()


# In[ ]:

#dates_sum.tail()


# In[ ]:

#type(dates_sum.days_int[0])


# In[ ]:

#len(dates_sum.category.unique())


# In[ ]:

cust_repur = dates_sum


# In[ ]:

#cust_repur.head()


# In[ ]:

cust_repur.shape


# In[ ]:

#len(cust_repur.category.unique())


# In[ ]:

del cust_repur['time']


# In[ ]:

cust_repur.head()


# In[ ]:

cust_repur.reset_index(drop=True, inplace=True)


# In[ ]:

cust_repur.head()


# In[ ]:

cust_repur.shape


# In[ ]:

list_of_indexes = [np.argmax(g['timestamp']) for l, g in cust_repur.groupby(['cust_id','category'])]


# In[ ]:

cust_repur_original = cust_repur.ix[list_of_indexes]

print "last purchase date"

# In[ ]:

#cust_repur_original.head()


# In[ ]:

cust_repur_original.reset_index(drop=True,inplace=True)


# In[ ]:

#cust_repur_original.head()


# In[ ]:

cust_repur_original['new_date'] = ""


# In[ ]:

cust_repur_original.head()


# In[ ]:

for x in range(0,len(cust_repur_original)):
    if cust_repur_original.days_int[x] != 0:
        cust_repur_original.new_date[x] = cust_repur_original.days_int[x] - 1
    else:
        cust_repur_original.new_date[x] = 0


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['new_count'] = cust_repur_original['counter'] - 1


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['repurchase_time'] = cust_repur_original['days_int'] / cust_repur_original['new_count']


# In[ ]:

#cust_repur_original.head()


# In[ ]:

#len(cust_repur_original.category.unique())

print "repurchase time is calculated"
# In[ ]:

abc = pd.pivot_table(cust_repur_original, index=['cust_id'], columns=['category'], values=['repurchase_time'], aggfunc='first')


# In[ ]:

abc.shape


# In[ ]:

#abc.head()


# In[ ]:

#type(abc)


# In[ ]:

mi = abc.columns


# In[ ]:

mi.tolist()


# In[ ]:

ind = pd.Index([e[1] for e in mi.tolist()])


# In[ ]:

abc.columns = ind


# In[ ]:

#abc.head()


# In[ ]:

categories = pd.DataFrame(abc.columns)


# In[ ]:

categories.rename(columns={0:'category'}, inplace=True)

print "calculating art for each category"
# In[ ]:

art = abc[categories.category[categories.index]].sum() / abc[categories.category[categories.index]].count()


# In[ ]:

type(art)


# In[ ]:

art = art.to_frame()


# In[ ]:

art.reset_index(inplace=True)


# In[ ]:

art.rename(columns={'index':'category'}, inplace=True)


# In[ ]:

art.rename(columns={0:'average_repurchase_time'}, inplace=True)


# In[ ]:

art

print "art is completed"
# In[ ]:

cust_repur_original.head()


# In[ ]:

del cust_repur_original['sum_days']


# In[ ]:

del cust_repur_original['new_date']


# In[ ]:

del cust_repur_original['new_count']


# In[ ]:

cust_repur_original.head()


# In[ ]:

######cust_repur_original["timestamp_new"] = pd.to_datetime(cust_repur_original["timestamp"]).apply(lambda x: x.replace(hour =0,minute=0, second=0))


# In[ ]:

cust_repur_original['time_diff'] = ""


# In[ ]:

cust_repur_original.head()


# In[ ]:

import datetime
for x in range(0,len(cust_repur_original)):
    cust_repur_original.time_diff[x]= datetime.datetime.now().date() - cust_repur_original.timestamp[x].to_datetime().date()


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original.tail()


# In[ ]:

cust_repur_original.counter = cust_repur_original.counter.fillna(0)


# In[ ]:

cust_repur_original.head()


# In[ ]:

art_dict = art.set_index('category')['average_repurchase_time'].to_dict()


# In[ ]:

art_dict


# In[ ]:

for x in range(0,len(cust_repur_original)):
    if cust_repur_original.counter[x] == 0:
        if art_dict.has_key(str(cust_repur_original.category[x])):
            cust_repur_original.repurchase_time[x] = art_dict[str(cust_repur_original.category[x])]


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original.tail()


# In[ ]:

cust_repur_original['time_diff_int'] = ""


# In[ ]:

for x in range(0,len(cust_repur_original)):
    cust_repur_original.time_diff_int[x] = cust_repur_original.time_diff[x].days


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original['depriotarize'] = ""


# In[ ]:

cust_repur_original.head()


# In[ ]:

cust_repur_original.repurchase_time = cust_repur_original.repurchase_time.fillna(0)


# In[ ]:

cust_repur_original.head()


# In[ ]:

type(cust_repur_original.time_diff[1])


# In[ ]:

cust_repur_original.head()


# In[ ]:

for x in range(0,len(cust_repur_original)):
    if cust_repur_original.time_diff_int[x] > cust_repur_original.repurchase_time[x]:
        cust_repur_original.depriotarize[x] = "Recommend"
    else:
        cust_repur_original.depriotarize[x] = "Do_not_Recommend"


# In[ ]:

cust_repur_original.head()


# In[ ]:

del cust_repur_original['timestamp']


# In[ ]:

del cust_repur_original['counter']


# In[ ]:

del cust_repur_original['days_int']


# In[ ]:

del cust_repur_original['repurchase_time']


# In[ ]:

del cust_repur_original['time_diff']


# In[ ]:

del cust_repur_original['time_diff_int']


# In[ ]:

cust_repur_original.head()

print "completed"