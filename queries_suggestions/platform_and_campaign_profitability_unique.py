from sqlalchemy import create_engine
import pandas as pd
engine = create_engine('postgresql://postgres_user:admin@paktortest.tuple-mia.com:5432/postgres_db')
trigger_table = pd.read_sql_table(table_name="trigger_campaign_details", con=engine)
experiment_table = pd.read_sql_table(table_name="experiment_campaign_log", con=engine)
master_table = pd.read_sql_table(table_name="master_table", con=engine)


######################### Calculating Profitability for Platforms #####################################

platforms = trigger_table.platform.unique()
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

platform_dict = {}
for c in range(len(platforms)):
    platform_dict[platforms[c]] = platform_prof(unique_platform=platforms[c])


platform_profitability = pd.DataFrame({"platforms": platform_dict.keys(), "profitability": platform_dict.values()})
platform_profitability['relative_profitability'] = (platform_profitability.profitability * 100) / platform_profitability.profitability.sum()
platform_profitability.to_sql("platform_profitability", con=engine, if_exists="replace", index=False)

######################### Calculating Profitability for Campaigns #####################################

campaigns = trigger_table.campaign_type.unique()
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

campaign_dict = {}
for c in range(len(campaigns)):
    campaign_dict[campaigns[c]] = campaign_prof(unique_campaign=campaigns[c])


campaign_profitability = pd.DataFrame({"campaigns": campaign_dict.keys(), "profitability": campaign_dict.values()})
campaign_profitability['relative_profitability'] = (campaign_profitability.profitability * 100) / (campaign_profitability.profitability.sum())

from sqlalchemy.dialects.postgresql import INTEGER
campaign_profitability.to_sql("campaign_profitability", con=engine, if_exists="replace", index=False, dtype={'campaigns':INTEGER})
















