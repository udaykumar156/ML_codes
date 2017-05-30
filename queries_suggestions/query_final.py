from sqlalchemy import create_engine
engine = create_engine('************************************************************')

import pandas as pd
master_table = pd.read_sql_table(table_name="master_table", con=engine)
prediction_period = pd.read_sql_table(table_name="predict_period", con=engine)

################ Original Queries ########################

qu1_dict = {'query':master_table.query('value == "Very High" and churn == 1'),'original_query':'value = Very High and churn = 1'}
qu2_dict = {'query':master_table.query('value in ("Very High","High") and engagement in ("Low","Medium")'),'original_query':'value = Very High or High and engagement = Low or Medium'}
qu3_dict = {'query':master_table.query('bio == 0 and lifestage == "New"'),'original_query':'bio = 0 and lifestage = New'}
qu4_dict = {'query':master_table.query('lifestage == "Retained" and churn == 0'),'original_query':'lifestage = Retained and churn = 0'}
qu5_dict = {'query':master_table.query('high_convertor in ("Very High", "High") and engagement in ("Very High", "High")'),'original_query':'high_convertor = Very High or High and engagement = Very High or High'}
qu6_dict = {'query':master_table.query('high_convertor in ("Low", "Very Low") and engagement in ("Very Low", "Low")'),'original_query':'high_convertor = Low or Very Low and engagement = Very Low or Low'}
qu7_dict = {'query':master_table.query('high_convertor in ("High", "Very High") and churn == 1'),'original_query':'high_convertor = High or Very High and churn == 1'}
qu8_dict = {'query':master_table.query('lifestage == "Reactivated" and high_convertor in ("High", "Very High")'),'original_query':'lifestage = Reactivated and high_convertor = High or Very High'}
qu9_dict = {'query':master_table.query('lifestage == "Reactivated" and value in ("Very High", "High")'),'original_query':'lifestage = Reactivated and value = Very High or High'}

############## Customers ######################

qu1_dict['qu1_cust'] = len(qu1_dict['query'].cust_id.unique())
qu2_dict['qu2_cust'] = len(qu2_dict['query'].cust_id.unique())
qu3_dict['qu3_cust'] = len(qu3_dict['query'].cust_id.unique())
qu4_dict['qu4_cust'] = len(qu4_dict['query'].cust_id.unique())
qu5_dict['qu5_cust'] = len(qu5_dict['query'].cust_id.unique())
qu6_dict['qu6_cust'] = len(qu6_dict['query'].cust_id.unique())
qu7_dict['qu7_cust'] = len(qu7_dict['query'].cust_id.unique())
qu8_dict['qu8_cust'] = len(qu8_dict['query'].cust_id.unique())
qu9_dict['qu9_cust'] = len(qu9_dict['query'].cust_id.unique())

################## Query View ########################

qu1_dict['qu1_view'] = "Customers with value Very High and Churn is Zero"
qu2_dict['qu2_view'] = "Customers with value Very High or High and Engagement Low or Medium"
qu3_dict['qu3_view'] = "Customers with Bio as Zero and Lifestage as New"
qu4_dict['qu4_view'] = "Customers whose Lifestage is Retained and Churn is Zero"
qu5_dict['qu5_view'] = "Customers with high_convertor Very High and High and engagement Very High or High"
qu6_dict['qu6_view'] = "Customers with high_convertor Low or Very Low and engagement Very Low or Low"
qu7_dict['qu7_view'] = "Customers with high_convertor High or Very High and Churn is One"
qu8_dict['qu8_view'] = "Customers whose Lifestage is Reactivated and High Convertion"
qu9_dict['qu9_view'] = "Customers whose Lifestage is Reactivated and with Very High and High value"

############# Description ######################

qu1_dict['qu1_desc'] = ["Found <strong>{value1}</strong> customers of potentially <strong>High value</strong> who are about to <strong>churn</strong> in next <strong>{value2} days</strong>".format(value1 = qu1_dict['qu1_cust'], value2 = int(prediction_period.predict_period[0])), "consider doing a rention campaign for them"]
qu2_dict['qu2_desc'] = ["Found <strong>{value1}</strong> customers with <strong>Very High</strong> and <strong>High</strong> value and engagement <strong>Low</strong> and <strong>Medium</strong> who are about to <strong>churn</strong> in next <strong>{value2} days</strong> ".        format(value1 = qu2_dict['qu2_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu3_dict['qu3_desc'] = ["Found <strong>{value1}</strong> customers with <strong>Bio</strong> as <strong>Zero</strong> and <strong>Lifestage</strong> is <strong>New</strong> who are about to <strong>churn</strong> in next <strong>{value2} days</strong> ".        format(value1 = qu3_dict['qu3_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu4_dict['qu4_desc'] = ["Found <strong>{value1}</strong> customers whoes <strong>lifestage</strong> is <strong>Retained</strong> and <strong>churn</strong> is <strong>zero</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".            format(value1 = qu4_dict['qu4_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu5_dict['qu5_desc'] = ["Found <strong>{value1}</strong> customers of potentially <strong>Very High and High convertor</strong> and <strong>Very High and High engagement</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".        format(value1 = qu5_dict['qu5_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu6_dict['qu6_desc'] = ["Found <strong>{value1}</strong> customers of potentially <strong>Low and Very Low convertor</strong> and <strong>Very Low and Low engagement</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".        format(value1 = qu6_dict['qu6_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu7_dict['qu7_desc'] =[ "Found <strong>{value1}</strong> customers of potentially <strong>High and Very High convertor</strong> and <strong>churn is one</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".        format(value1 = qu7_dict['qu7_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu8_dict['qu8_desc'] = ["Found <strong>{value1}</strong> customers whoes <strong>Lifestage</strong> is <strong>Reactivated</strong> and having <strong>High and Very High Convertion</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".        format(value1 = qu8_dict['qu8_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]
qu9_dict['qu9_desc'] = ["Found <strong>{value1}</strong> customers whoes <strong>Lifestage</strong> is <strong>Reactivated</strong> and having <strong>High and Low value</strong> are about to <strong>churn</strong> in next <strong>{value2} days</strong> consider doing a rention campaign for them".        format(value1 = qu9_dict['qu9_cust'], value2 = int(prediction_period.predict_period[0])),"consider doing a rention campaign for them"]

############ Graph and Revenue ###################

from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres_user:admin@localhost:5432/postgres_db')
import pandas as pd

def calculateProfit(queryDict):
    master_table = pd.read_sql_table(table_name="master_table", con=engine)
    personal_table = pd.read_sql_table(table_name="personal_table", con=engine)
    qu1_dict = queryDict['query']
    if len(queryDict['query'].cust_id.unique()) :
           profitability_query = queryDict['query'].cltv.sum() / len(queryDict['query'].cust_id.unique())
    else:
           profitability_query = 0
           
    profitability_master = master_table.cltv.sum() / len(master_table.cust_id.unique())
    master_other = pd.concat([master_table, queryDict['query'], queryDict['query']]).drop_duplicates(keep=False)
    profitability_other = master_other.cltv.sum() / len(master_other.cust_id.unique())
    query_cust = queryDict['query'].cust_id.tolist()
    query_personal = personal_table[personal_table['cust_id'].isin(query_cust)]
    import lepl.apps.rfc3696
    email_validator = lepl.apps.rfc3696.Email()
    valid_customers_count = 0
    for x in query_personal.email_id:
        if not email_validator(x.replace(' ','')):
            print "x"
        else:
            valid_customers_count = valid_customers_count + 1
    if valid_customers_count:
        estimated_reach = len(query_personal.cust_id.unique()) / valid_customers_count * 100
    else:
        estimated_reach = 0
    convertion = (valid_customers_count * 10) / 100
    estimated_revenue = convertion * profitability_query
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


query_dict = {"query_view":[qu1_dict['qu1_view'],qu2_dict['qu2_view'],qu3_dict['qu3_view'],qu4_dict['qu4_view'],qu5_dict['qu5_view'],qu6_dict['qu6_view'],qu7_dict['qu7_view'],qu8_dict['qu8_view'],qu9_dict['qu9_view']],               "query_desc":[qu1_dict['qu1_desc'],qu2_dict['qu2_desc'],qu3_dict['qu3_desc'],qu4_dict['qu4_desc'],qu5_dict['qu5_desc'],qu6_dict['qu6_desc'],qu7_dict['qu7_desc'],qu8_dict['qu8_desc'],qu9_dict['qu9_desc']],               "revenue":[qu1_dict['qu1_rev'],qu2_dict['qu2_rev'],qu3_dict['qu3_rev'],qu4_dict['qu4_rev'],qu5_dict['qu5_rev'],qu6_dict['qu6_rev'],qu7_dict['qu7_rev'], qu8_dict['qu8_rev'],qu9_dict['qu9_rev']],               "customer_count":[qu1_dict["qu1_cust"],qu2_dict["qu2_cust"],qu3_dict["qu3_cust"],qu4_dict["qu4_cust"],qu5_dict["qu5_cust"],qu6_dict["qu6_cust"],qu7_dict["qu7_cust"],qu8_dict["qu8_cust"],qu9_dict["qu9_cust"]],               "query_real":[qu1_dict['original_query'],qu2_dict['original_query'],qu3_dict['original_query'],qu4_dict['original_query'],qu5_dict['original_query'],qu6_dict['original_query'],qu7_dict['original_query'],qu8_dict['original_query'],qu9_dict['original_query']],               "status":[True,True,True,True,True,True,True,True,True],              "id":[1,2,3,4,5,6,7,8,9],              "estimate":[qu1_dict['qu1_reach'],qu2_dict['qu2_reach'],qu3_dict['qu3_reach'],qu4_dict['qu4_reach'],qu5_dict['qu5_reach'],qu6_dict['qu6_reach'],qu7_dict['qu7_reach'], qu8_dict['qu8_reach'],qu9_dict['qu9_reach']],               "prof_query":[qu1_dict['profitability_qu1'],qu2_dict['profitability_qu2'],qu3_dict['profitability_qu3'],qu4_dict['profitability_qu4'],qu5_dict['profitability_qu5'],qu6_dict['profitability_qu6'],qu7_dict['profitability_qu7'],qu8_dict['profitability_qu8'],qu9_dict['profitability_qu9']],               "prof_master":[qu1_dict['profitability_master'],qu2_dict['profitability_master'],qu3_dict['profitability_master'],qu4_dict['profitability_master'],qu5_dict['profitability_master'],qu6_dict['profitability_master'],qu7_dict['profitability_master'],qu8_dict['profitability_master'],qu9_dict['profitability_master']],               "prof_others":[qu1_dict['profitability_other'],qu2_dict['profitability_other'],qu3_dict['profitability_other'],qu4_dict['profitability_other'],qu5_dict['profitability_other'],qu6_dict['profitability_other'],qu7_dict['profitability_other'],qu8_dict['profitability_other'],qu9_dict['profitability_other']]}
query_table = pd.DataFrame(query_dict)

from sqlalchemy.dialects.postgresql import ARRAY, TEXT

query_table.to_sql("suggestions_suggestion", if_exists="replace", index = False, con=engine,
dtype={'query_desc':ARRAY(TEXT, dimensions=1)})




