from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres_user:admin@paktortest.tuple-mia.com:5432/postgres_db')
from mapping import doc
import pandas as pd


trigger_table = pd.read_sql_table(table_name="trigger_campaign_details", con=engine)
mailchimp_table = pd.read_sql_table(table_name="build_mailchimp_campaign_info", con=engine)
master_table = pd.read_sql_table(table_name="master_table", con=engine)
prediction_period = pd.read_sql_table(table_name="predict_period", con=engine)
personal_table = pd.read_csv('/home/ubuntu/tuple_client/personal_table.csv', usecols=['cust_id', 'email_id'])

bio_table = pd.read_sql_table(table_name="profile_cl", con=engine)
max_index = bio_table.ARPU.argmax()
max_bio = bio_table.cluster.ix[max_index]


trigger_mailchimp = trigger_table.query('platform == "mailchimp"')
max_index = trigger_mailchimp.created_date.argmax()
unique_camp_id = trigger_mailchimp.campaign_id.ix[max_index]


mailchimp_data = mailchimp_table.query('open == "No" and campaign_id == @unique_camp_id')
mailchimp_data = mailchimp_data[['open', 'cust_id', 'campaign_id']]
mailchimp_master = pd.merge(master_table, mailchimp_data, on='cust_id', how='right')

############################### Query's ######################

qu1_dict = {'query':master_table.query('value == "Very High" and churn == 1 and lifestage != "Lapsed"'),'original_query':'value = Very High and churn = 1 and lifestage != Lapsed'}
qu2_dict = {'query':master_table.query('value in ("Very High","High") and engagement in ("Low","Medium") and lifestage != "Lapsed"'),'original_query':'value = Very High or value = High and engagement = Low or engagement = Medium and lifestage != Lapsed'}
qu3_dict = {'query':master_table.query('bio == @max_bio and lifestage == "New"'),'original_query':'bio = %s and lifestage = New'%max_bio}
qu4_dict = {'query':master_table.query('lifestage == "Retained" and churn == 0'),'original_query':'lifestage = Retained and churn = 0'}
qu5_dict = {'query':master_table.query('high_convertor in ("Very High", "High") and engagement in ("Very High", "High") and lifestage != "Lapsed"'),'original_query':'high_convertor = Very High or high_convertor = High and engagement = Very High or engagement = High and lifestage != Lapsed'}
qu6_dict = {'query':master_table.query('high_convertor in ("Low", "Very Low") and engagement in ("Very Low", "Low") and lifestage != "Lapsed"'),'original_query':'high_convertor = Low or high_convertor = Very Low and engagement = Very Low or engagement = Low and lifestage != Lapsed'}
qu7_dict = {'query':master_table.query('high_convertor in ("High", "Very High") and churn == 1 and lifestage != "Lapsed"'),'original_query':'high_convertor = High or high_convertor = Very High and churn = 1 and lifestage != Lapsed'}
qu8_dict = {'query':master_table.query('lifestage == "Reactivated" and high_convertor in ("High", "Very High")'),'original_query':'lifestage = Reactivated and high_convertor = High or high_convertor = Very High'}
qu9_dict = {'query':master_table.query('lifestage == "Reactivated" and value in ("Very High", "High")'),'original_query':'lifestage = Reactivated and value = Very High or value = High'}
qu10_dict = {'query':mailchimp_master.query('open == "No" and campaign_id == @unique_camp_id and lifestage != "Lapsed"'),'original_query':'open = No and campaign_id = %s and lifestage != Lapsed'%unique_camp_id}

################# Customer Count #########################

qu1_dict['qu1_cust'] = len(qu1_dict['query'].cust_id.unique())
qu2_dict['qu2_cust'] = len(qu2_dict['query'].cust_id.unique())
qu3_dict['qu3_cust'] = len(qu3_dict['query'].cust_id.unique())
qu4_dict['qu4_cust'] = len(qu4_dict['query'].cust_id.unique())
qu5_dict['qu5_cust'] = len(qu5_dict['query'].cust_id.unique())
qu6_dict['qu6_cust'] = len(qu6_dict['query'].cust_id.unique())
qu7_dict['qu7_cust'] = len(qu7_dict['query'].cust_id.unique())
qu8_dict['qu8_cust'] = len(qu8_dict['query'].cust_id.unique())
qu9_dict['qu9_cust'] = len(qu9_dict['query'].cust_id.unique())
qu10_dict['qu10_cust'] = len(qu10_dict['query'].cust_id.unique())


#################### Query View #####################


qu1_dict['qu1_view'] = "Customers with Value - Very High and Churn is Zero"
qu2_dict['qu2_view'] = "Customers with Value - High and Engaged - Low"
qu3_dict['qu3_view'] = "Customers with Bio as %s and Lifestage as New"%max_bio
qu4_dict['qu4_view'] = "Customers whose Lifestage is Retained and Churn is Zero"
qu5_dict['qu5_view'] = "Customers with Potential - High and Engaged - High"
qu6_dict['qu6_view'] = "Customers with Potential - Low and  Engaged - Low"
qu7_dict['qu7_view'] = "Customers with Potential - High and Churn is One"
qu8_dict['qu8_view'] = "Customers whose Lifestage is Reactivated and Potential - High"
qu9_dict['qu9_view'] = "Customers whose Lifestage is Reactivated and Value - High"
qu10_dict['qu10_view'] = "Customers who have not opened Email"


################## Query Description #################

qu1_dict['qu1_desc'] = ["Found <strong>{value1}</strong> customers of potentially <strong>High value</strong> who are about to <strong>churn</strong> in next <strong>{value2} days</strong>".format(value1 = qu1_dict['qu1_cust'], value2 = int(prediction_period.predict_period[0])), "Consider doing a retention campaign for them"]
qu2_dict['qu2_desc'] = ["<strong>{value1}</strong> customers with <strong>Very High</strong> and <strong>High</strong> value and <strong>Low</strong> and <strong>Medium</strong> engagement are likely to be moderately engaged in next <strong>{value2} days</strong> ".format(value1 = qu2_dict['qu2_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a retention campaign for them"]
qu3_dict['qu3_desc'] = ["Found <strong>{value1}</strong> customers with <strong>Bio</strong> as <strong>{value2}</strong> and <strong>Lifestage</strong> is <strong>New</strong> will participate in any event in next <strong>{value3} days</strong> ".format(value1 = qu3_dict['qu3_cust'],value2=max_bio,value3 = int(prediction_period.predict_period[0])),"Consider doing a discount campaign for them"]
qu4_dict['qu4_desc'] = ["<strong>{value1}</strong> customers <strong>lifestage</strong> is <strong>Retained</strong> and <strong>churn</strong> is <strong>zero</strong> and are about to <strong>churn</strong> in next <strong>{value2} days</strong>".format(value1 = qu4_dict['qu4_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a cross-sell recommendation for them"]
qu5_dict['qu5_desc'] = ["<strong>{value1}</strong> customers of potentially <strong>Very High and High convertor</strong> and <strong>Very High and High engagement</strong> are about to engage extremly in next <strong>{value2} days</strong>".format(value1 = qu5_dict['qu5_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a retention campaign for them"]
qu6_dict['qu6_desc'] = ["<strong>{value1}</strong> customers of potentially <strong>Low and Very Low convertor</strong> and <strong>Very Low and Low engagement</strong> are about to get disengaged in next <strong>{value2} days</strong>".format(value1 = qu6_dict['qu6_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a retention campaign for them"]
qu7_dict['qu7_desc'] =[ "Found <strong>{value1}</strong> customers of potentially <strong>High and Very High convertor</strong> and <strong>churn is one</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong>".format(value1 = qu7_dict['qu7_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a discount campaign for them"]
qu8_dict['qu8_desc'] = ["Found <strong>{value1}</strong> customers whoes <strong>Lifestage</strong> is <strong>Reactivated</strong> and having <strong>High and Very High Convertion</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong>".format(value1 = qu8_dict['qu8_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a retention campaign for them"]
qu9_dict['qu9_desc'] = ["Found <strong>{value1}</strong> customers whoes <strong>Lifestage</strong> is <strong>Reactivated</strong> and having <strong>High and Low value</strong> will not engage effectively in next <strong>{value2} days</strong>".format(value1 = qu9_dict['qu9_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing a discount campaign for them"]
qu10_dict['qu10_desc'] = ["Found <strong>{value1}</strong> customers who have not opened your <strong>email campaign</strong> in last <strong>{value2} days</strong>".format(value1 = qu10_dict['qu10_cust'], value2 = int(prediction_period.predict_period[0])),"Consider doing an Adwords campaign for them"]


################## Profotability ###################

def calculateProfit(queryDict):
    qu1_dict = queryDict['query']
    if len(queryDict['query'].cust_id.unique()) :
           profitability_query = queryDict['query'].cltv.sum() / len(queryDict['query'].cust_id.unique())
    else:
           profitability_query = 0
           
    profitability_master = master_table.cltv.sum() / len(master_table.cust_id.unique())
    master_other = pd.concat([master_table, queryDict['query'], queryDict['query']]).drop_duplicates(keep=False)
    profitability_other = master_other.cltv.sum() / len(master_other.cust_id.unique())
    query_cust = queryDict['query'].cust_id.tolist()
    query_personal = personal_table[personal_table['cust_id'].isin(query_cust)].drop_duplicates()
    import lepl.apps.rfc3696
    email_validator = lepl.apps.rfc3696.Email()
    valid_customers_count = 0
    for x in query_personal.email_id:
        if x is not None and email_validator(str(x).replace(' ','')):
            valid_customers_count = valid_customers_count + 1          
            
    estimated_reach = valid_customers_count
    convertion = (valid_customers_count * 10) / 100
    estimated_revenue = int(round(convertion * profitability_query))
    return profitability_query,profitability_master,profitability_other,estimated_revenue,estimated_reach


qu1_dict['profitability_qu1'],qu1_dict['profitability_master'],qu1_dict['profitability_other'],qu1_dict['qu1_rev'], qu1_dict['qu1_reach'] = calculateProfit(qu1_dict)
qu2_dict['profitability_qu2'],qu2_dict['profitability_master'],qu2_dict['profitability_other'],qu2_dict['qu2_rev'], qu2_dict['qu2_reach'] = calculateProfit(qu2_dict)
qu3_dict['profitability_qu3'],qu3_dict['profitability_master'],qu3_dict['profitability_other'],qu3_dict['qu3_rev'], qu3_dict['qu3_reach'] = calculateProfit(qu3_dict)
qu4_dict['profitability_qu4'],qu4_dict['profitability_master'],qu4_dict['profitability_other'],qu4_dict['qu4_rev'], qu4_dict['qu4_reach'] = calculateProfit(qu4_dict)
qu5_dict['profitability_qu5'],qu5_dict['profitability_master'],qu5_dict['profitability_other'],qu5_dict['qu5_rev'], qu5_dict['qu5_reach'] = calculateProfit(qu5_dict)
qu6_dict['profitability_qu6'],qu6_dict['profitability_master'],qu6_dict['profitability_other'],qu6_dict['qu6_rev'], qu6_dict['qu6_reach'] = calculateProfit(qu6_dict)
qu7_dict['profitability_qu7'],qu7_dict['profitability_master'],qu7_dict['profitability_other'],qu7_dict['qu7_rev'], qu7_dict['qu7_reach'] = calculateProfit(qu7_dict)
qu8_dict['profitability_qu8'],qu8_dict['profitability_master'],qu8_dict['profitability_other'],qu8_dict['qu8_rev'], qu8_dict['qu8_reach'] = calculateProfit(qu8_dict)
qu9_dict['profitability_qu9'],qu9_dict['profitability_master'],qu9_dict['profitability_other'],qu9_dict['qu9_rev'], qu9_dict['qu9_reach'] = calculateProfit(qu9_dict)
qu10_dict['profitability_qu10'],qu10_dict['profitability_master'],qu10_dict['profitability_other'],qu10_dict['qu10_rev'], qu10_dict['qu10_reach'] = calculateProfit(qu10_dict)


################## Reading data from CSV ####################

trans_users = pd.read_csv('/home/ubuntu/tuple_client/%s.csv'%doc['table_map']['TRANSACTION_MASTER'],sep='|',usecols=[doc['column_map']['TRANSACTION_MASTER']['cust_id']])
trans_users.rename(columns={doc['column_map']['TRANSACTION_MASTER']['cust_id']:'cust_id'}, inplace=True)
event_users = pd.read_csv('/home/ubuntu/tuple_client/%s.csv'%doc['table_map']['EVENT_LOG'],sep = '|', usecols=[doc['column_map']['EVENT_LOG']['cust_id']])
event_users.rename(columns={doc['column_map']['EVENT_LOG']['cust_id']:'cust_id'}, inplace=True)
trans_event_users = pd.concat([trans_users, event_users], axis=0)


################## Calculating Visits ###################

def calculateavgVisits(queryDF):
    qu1 = queryDF['query']
    query_cust = qu1['cust_id']
    qu_trans = trans_event_users[trans_event_users['cust_id'].isin(query_cust)]
    qu_cust_count = qu_trans.groupby('cust_id').size().reset_index().rename(columns = {0:'counter'})
    if len(qu_cust_count.cust_id.unique()):
        avg_visits_query = qu_cust_count.counter.sum() / len(qu_cust_count.cust_id.unique())
    else:
        avg_visits_query = 0
    master_cust = master_table['cust_id']
    master_count = trans_event_users[trans_event_users['cust_id'].isin(master_cust)]
    master_cust_count = master_count.groupby('cust_id').size().reset_index().rename(columns = {0:'counter'})
    if len(master_cust_count.cust_id.unique()):
        avg_visits_master = master_cust_count.counter.sum() / len(master_cust_count.cust_id.unique())
    else :
        avg_visits_master = 0
    others_count = trans_event_users[~trans_event_users.cust_id.isin(query_cust)]
    others_cust_count = others_count.groupby('cust_id').size().reset_index().rename(columns = {0:'counter'})
    if len(others_cust_count.cust_id.unique()):
        avg_visits_others = others_cust_count.counter.sum() / len(others_cust_count.cust_id.unique())
    else:
        avg_visits_others = 0
    return avg_visits_query,avg_visits_master,avg_visits_others


qu1_dict['visits_qu1'],qu1_dict['visits_master'],qu1_dict['visits_other'] = calculateavgVisits(qu1_dict)
qu2_dict['visits_qu2'],qu2_dict['visits_master'],qu2_dict['visits_other'] = calculateavgVisits(qu2_dict)
qu3_dict['visits_qu3'],qu3_dict['visits_master'],qu3_dict['visits_other'] = calculateavgVisits(qu3_dict)
qu4_dict['visits_qu4'],qu4_dict['visits_master'],qu4_dict['visits_other'] = calculateavgVisits(qu4_dict)
qu5_dict['visits_qu5'],qu5_dict['visits_master'],qu5_dict['visits_other'] = calculateavgVisits(qu5_dict)
qu6_dict['visits_qu6'],qu6_dict['visits_master'],qu6_dict['visits_other'] = calculateavgVisits(qu6_dict)
qu7_dict['visits_qu7'],qu7_dict['visits_master'],qu7_dict['visits_other'] = calculateavgVisits(qu7_dict)
qu8_dict['visits_qu8'],qu8_dict['visits_master'],qu8_dict['visits_other'] = calculateavgVisits(qu8_dict)
qu9_dict['visits_qu9'],qu9_dict['visits_master'],qu9_dict['visits_other'] = calculateavgVisits(qu9_dict)
qu10_dict['visits_qu10'],qu10_dict['visits_master'],qu10_dict['visits_other'] = calculateavgVisits(qu10_dict)



###################### Creating DataFrame ###################

query_dict = {"query_view":[qu1_dict['qu1_view'],qu2_dict['qu2_view'],qu3_dict['qu3_view'],qu4_dict['qu4_view'],qu5_dict['qu5_view'],qu6_dict['qu6_view'],qu7_dict['qu7_view'],qu8_dict['qu8_view'],qu9_dict['qu9_view'],qu10_dict['qu10_view']],               "query_desc":[qu1_dict['qu1_desc'],qu2_dict['qu2_desc'],qu3_dict['qu3_desc'],qu4_dict['qu4_desc'],qu5_dict['qu5_desc'],qu6_dict['qu6_desc'],qu7_dict['qu7_desc'],qu8_dict['qu8_desc'],qu9_dict['qu9_desc'],qu10_dict['qu10_desc']],               "revenue":[qu1_dict['qu1_rev'],qu2_dict['qu2_rev'],qu3_dict['qu3_rev'],qu4_dict['qu4_rev'],qu5_dict['qu5_rev'],qu6_dict['qu6_rev'],qu7_dict['qu7_rev'], qu8_dict['qu8_rev'],qu9_dict['qu9_rev'],qu10_dict['qu10_rev']],               "customer_count":[qu1_dict["qu1_cust"],qu2_dict["qu2_cust"],qu3_dict["qu3_cust"],qu4_dict["qu4_cust"],qu5_dict["qu5_cust"],qu6_dict["qu6_cust"],qu7_dict["qu7_cust"],qu8_dict["qu8_cust"],qu9_dict["qu9_cust"],qu10_dict["qu10_cust"]],               "query_real":[qu1_dict['original_query'],qu2_dict['original_query'],qu3_dict['original_query'],qu4_dict['original_query'],qu5_dict['original_query'],qu6_dict['original_query'],qu7_dict['original_query'],qu8_dict['original_query'],qu9_dict['original_query'],qu10_dict['original_query']],               "status":[True,True,True,True,True,True,True,True,True, True],              "id":[1,2,3,4,5,6,7,8,9,10],              "estimate":[qu1_dict['qu1_reach'],qu2_dict['qu2_reach'],qu3_dict['qu3_reach'],qu4_dict['qu4_reach'],qu5_dict['qu5_reach'],qu6_dict['qu6_reach'],qu7_dict['qu7_reach'], qu8_dict['qu8_reach'],qu9_dict['qu9_reach'],qu10_dict['qu10_reach']],               "prof_query":[qu1_dict['profitability_qu1'],qu2_dict['profitability_qu2'],qu3_dict['profitability_qu3'],qu4_dict['profitability_qu4'],qu5_dict['profitability_qu5'],qu6_dict['profitability_qu6'],qu7_dict['profitability_qu7'],qu8_dict['profitability_qu8'],qu9_dict['profitability_qu9'],qu10_dict['profitability_qu10']],               "prof_master":[qu1_dict['profitability_master'],qu2_dict['profitability_master'],qu3_dict['profitability_master'],qu4_dict['profitability_master'],qu5_dict['profitability_master'],qu6_dict['profitability_master'],qu7_dict['profitability_master'],qu8_dict['profitability_master'],qu9_dict['profitability_master'],qu10_dict['profitability_master']],               "prof_others":[qu1_dict['profitability_other'],qu2_dict['profitability_other'],qu3_dict['profitability_other'],qu4_dict['profitability_other'],qu5_dict['profitability_other'],qu6_dict['profitability_other'],qu7_dict['profitability_other'],qu8_dict['profitability_other'],qu9_dict['profitability_other'],qu10_dict['profitability_other']],
              "visits_query":[qu1_dict['visits_qu1'],qu2_dict['visits_qu2'],qu3_dict['visits_qu3'],qu4_dict['visits_qu4'],qu5_dict['visits_qu5'],qu6_dict['visits_qu6'],qu7_dict['visits_qu7'],qu8_dict['visits_qu8'],qu9_dict['visits_qu9'],qu10_dict['visits_qu10']],
              "visits_master":[qu1_dict['visits_master'],qu2_dict['visits_master'],qu3_dict['visits_master'],qu4_dict['visits_master'],qu5_dict['visits_master'],qu6_dict['visits_master'],qu7_dict['visits_master'],qu8_dict['visits_master'],qu9_dict['visits_master'],qu10_dict['visits_master']],
              "visits_other":[qu1_dict['visits_other'],qu2_dict['visits_other'],qu3_dict['visits_other'],qu4_dict['visits_other'],qu5_dict['visits_other'],qu6_dict['visits_other'],qu7_dict['visits_other'],qu8_dict['visits_other'],qu9_dict['visits_other'],qu10_dict['visits_other']]}


query_table = pd.DataFrame(query_dict)


query_table['primary'] = ""
query_table['secondary'] = ""


query_table.primary.ix[0] = 'Value - HIGH'
query_table.secondary.ix[0] = 'Churn - HIGH'
query_table.primary.ix[1] = 'Value - HIGH'
query_table.secondary.ix[1] = 'Engaged - LOW'
query_table.primary.ix[2] = 'Potential - HIGH'
query_table.secondary.ix[2] = 'Lifestage - NEW'
query_table.primary.ix[3] = 'Lifestage - RETAINED'
query_table.secondary.ix[3] = 'Churn - LOW'
query_table.primary.ix[4] = 'Potential - HIGH'
query_table.secondary.ix[4] = 'Engaged - HIGH'
query_table.primary.ix[5] = 'Potential - LOW'
query_table.secondary.ix[5] = 'Engaged - LOW'
query_table.primary.ix[6] = 'Potential - HIGH'
query_table.secondary.ix[6] = 'Churn - HIGH'
query_table.primary.ix[7] = 'Lifestage - REACTIVATED'
query_table.secondary.ix[7] = 'Engaged - HIGH'
query_table.primary.ix[8] = 'Lifestage - REACTIVATED'
query_table.secondary.ix[8] = 'Value - HIGH'
query_table.primary.ix[9] = 'Email'
query_table.secondary.ix[9] = 'Open - No'


####################### Writing to DB #######################

from sqlalchemy.dialects.postgresql import ARRAY, TEXT
query_table.to_sql("suggestions_suggestion", if_exists="replace",index = False,con=engine,dtype={'query_desc':ARRAY(TEXT, dimensions=1)})















