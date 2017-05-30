
# coding: utf-8

# In[1]:

from sqlalchemy import create_engine


# In[2]:

engine = create_engine('*********************************************************')


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
    platforms_profitability_final = 0
    for x in platform_camp_id:
        experiment_camp = experiment_table[experiment_table.campaign_id.isin([x])]
        master_experiment = master_table[master_table.cust_id.isin(experiment_camp.cust_id.unique())]
        if not len(master_experiment.cust_id.unique()) :
            continue
        platforms_profitability_final = platforms_profitability_final + master_experiment.cltv.sum() / len(master_experiment.cust_id.unique())
    platforms_profitability_final = platforms_profitability_final/(len(platform_camp_id))
    return platforms_profitability_final


# In[9]:

platform_dict = {}
for c in range(len(platforms)):
    platform_dict[platforms[c]] = platform_prof(unique_platform=platforms[c])


# In[10]:

platform_profitability = pd.DataFrame({"platforms": platform_dict.keys(), "profitability": platform_dict.values()})


# In[11]:

platform_profitability['relative_profitability'] = (platform_profitability.profitability * 100) / platform_profitability.profitability.sum()


# In[12]:

platform_profitability


# In[13]:

platform_profitability.to_sql("platform_profitability", con=engine, if_exists="replace", index=False)


# In[14]:

def campaign_prof(unique_campaign):
    campaign_unique = trigger_table.query('campaign_type == @unique_campaign')
    campaign_camp_id = campaign_unique['campaign_id'].unique()
    campaign_profitability_final = 0
    for x in campaign_camp_id:
        experiment_camp = experiment_table[experiment_table.campaign_id.isin([x])]
        master_experiment = master_table[master_table.cust_id.isin(experiment_camp.cust_id.unique())]
        if not len(master_experiment.cust_id.unique()) :
            continue
        campaign_profitability_final = campaign_profitability_final + master_experiment.cltv.sum() / len(master_experiment.cust_id.unique())
    campaign_profitability_final = campaign_profitability_final/(len(campaign_camp_id))
    return campaign_profitability_final


# In[15]:

campaigns = trigger_table.campaign_type.unique()


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


# In[20]:

from sqlalchemy.dialects.postgresql import INTEGER


# In[21]:

campaign_profitability.to_sql("campaign_profitability", con=engine, if_exists="replace", index=False, dtype={'campaigns':INTEGER})


# In[ ]:




# In[ ]:




# In[ ]:



