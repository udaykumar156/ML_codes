
# coding: utf-8

# In[1]:

from sqlalchemy import create_engine


# In[2]:

engine = create_engine('postgresql://postgres_user:admin@paktortest.tuple-mia.com:5432/postgres_db')


# In[3]:

import pandas as pd


# In[4]:

trigger_table = pd.read_sql_table(table_name="trigger_campaign_details", con=engine)


# In[5]:

experiment_table = pd.read_sql_table(table_name="experiment_campaign_log", con=engine)


# In[6]:

master_table = pd.read_sql_table(table_name="master_table", con=engine)


# In[7]:

platforms = trigger_table.platform.unique()


# In[8]:

def platform_prof(unique_platform):
    platform_unique = trigger_table.query('platform == @unique_platform')
    platform_camp_id = platform_unique['campaign_id'].unique()
    experiment_camp = experiment_table[experiment_table.campaign_id.isin(platform_camp_id)]
    master_experiment = master_table[master_table.cust_id.isin(experiment_camp.cust_id.unique())]
    if len(master_experiment.cust_id.unique()):
        platforms_profitability = master_experiment.cltv.sum() / len(master_experiment.cust_id.unique())
    else:
        platforms_profitability = 0
    return platforms_profitability


# In[9]:

platform_dict = {}
for c in range(len(platforms)):
    platform_dict[platforms[c]] = platform_prof(unique_platform=platforms[c])


# In[10]:

platform_profitability = pd.DataFrame({"platforms": platform_dict.keys(), "profitability": platform_dict.values()})


# In[11]:

platform_profitability['relative_profitability'] = (platform_profitability.profitability * 100) / (platform_profitability.profitability.sum())


# In[12]:

platform_profitability


# In[13]:

platform_profitability.to_sql("platform_profitability", con=engine, if_exists="replace", index=False)


# In[14]:

campaigns = trigger_table.campaign_type.unique()


# In[15]:

def campaign_prof(unique_campaign):
    campaign_unique = trigger_table.query('campaign_type == @unique_campaign')
    campaign_id = campaign_unique['campaign_id'].unique()
    experiment_camp = experiment_table[experiment_table.campaign_id.isin(campaign_id)]
    master_experiment = master_table[master_table.cust_id.isin(experiment_camp.cust_id.unique())]
    if len(master_experiment.cust_id.unique()):
        campaign_profitability = master_experiment.cltv.sum() / len(master_experiment.cust_id.unique())
    else:
        campaign_profitability = 0
    return campaign_profitability


# In[16]:

campaign_dict = {}
for c in range(len(campaigns)):
    campaign_dict[campaigns[c]] = campaign_prof(unique_campaign=campaigns[c])


# In[17]:

campaign_profitability = pd.DataFrame({"campaigns": campaign_dict.keys(), "profitability": campaign_dict.values()})


# In[18]:

campaign_profitability['relative_profitability'] = (campaign_profitability.profitability * 100) / (campaign_profitability.profitability.sum())


# In[19]:

campaign_profitability


# In[22]:

from sqlalchemy.dialects.postgresql import INTEGER


# In[23]:

campaign_profitability.to_sql("campaign_profitability", con=engine, if_exists="replace", index=False, dtype={'campaigns':INTEGER})


# In[ ]:



