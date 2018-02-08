import pandas as pd
import numpy as np
import datetime as dt
from datetime import timedelta
import datetime
import psycopg2
from sqlalchemy import create_engine

class Connect:
    def __init__(self):
        self.conn = None
        self.cur = None

    def connect(self, database_info):
        try:
            self.conn = psycopg2.connect(
                database=database_info.database,
                user=database_info.username,
                password=database_info.password,
                host=database_info.host,
                port=database_info.port,
                sslmode='require'
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print e
            return None

    def connect_local(self):
        try:
            self.conn = psycopg2.connect(
                database='',
                user='',
                password='',
                host='',
                port='',
                sslmode=''
            )
            self.cur = self.conn.cursor()
        except Exception as e:
            print e
            return None

    def conn(self):
        return self.conn

    def cur(self):
        return self.cur


temp = Connect()
temp.connect_local()
try:
    query = 'SELECT * from postgres_db."public".master_table_history'
    temp.cur.execute(query)
    col_names = [desc[0] for desc in temp.cur.description]
    master_table_his = pd.DataFrame(temp.cur.fetchall(), columns=col_names)
    list_of_indexes_new = [np.argmax(g['custom_date_identifier']) for l, g in master_table_his.groupby(['cust_id'])]
    new_data = master_table_his.ix[list_of_indexes_new]
    new_data.reset_index(drop=True, inplace=True)
    new_data['custom_date_identifier'] = master_table_his['custom_date_identifier'].max()
    xyz = master_table_his.groupby(['cust_id', 'custom_date_identifier']).size().reset_index()
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
    old_data = pd.merge(master_table_his, dates, on=['cust_id', 'custom_date_identifier'], how='right')
    old_data['custom_date_identifier'] = master_table_his['custom_date_identifier'].max() - pd.Timedelta(days = 1)
    cltv_value_old = old_data[['cltv', 'custom_date_identifier']]
    old_cltv_sum = cltv_value_old.groupby('custom_date_identifier').sum().reset_index()
    old_cltv_count = cltv_value_old.groupby('custom_date_identifier').count().reset_index()
    old_cltv_avrg = old_cltv_sum.merge(old_cltv_count, on='custom_date_identifier')
    old_cltv_avrg.cltv_x = old_cltv_avrg.cltv_x.astype('float64', raise_on_error = False)
    old_cltv_avrg.cltv_y = old_cltv_avrg.cltv_y.astype('float64', raise_on_error = False)
    old_cltv_avrg['average_cltv'] = old_cltv_avrg.cltv_x / old_cltv_avrg.cltv_y.replace({ 0 : np.nan })
    cltv_value_new = new_data[['cltv', 'custom_date_identifier']]
    new_cltv_sum = cltv_value_new.groupby('custom_date_identifier').sum().reset_index()
    new_cltv_count = cltv_value_new.groupby('custom_date_identifier').count().reset_index()
    new_cltv_avrg = new_cltv_sum.merge(new_cltv_count, on='custom_date_identifier')
    new_cltv_avrg.cltv_x = new_cltv_avrg.cltv_x.astype('float64', raise_on_error = False)
    new_cltv_avrg.cltv_y = new_cltv_avrg.cltv_y.astype('float64', raise_on_error = False)
    new_cltv_avrg['average_cltv'] = new_cltv_avrg.cltv_x / new_cltv_avrg.cltv_y.replace({ 0 : np.nan })
    Nutshell_1 = "Average CLTV for yesterday was {value1}$ and {value2} by {value3}$ over previous day"    .format(value1 = int(round(new_cltv_avrg.average_cltv.sum())), value2 = "increased" if int(round(new_cltv_avrg.average_cltv.sum())) >= int(round(old_cltv_avrg.average_cltv.sum())) else "decreased", value3 = abs((int(round(new_cltv_avrg.average_cltv.sum())) - int(round(old_cltv_avrg.average_cltv.sum())))))
    counter_1 = 0
    counter_2 = 0
    for x in old_data.cust_id:
        if str(old_data.loc[old_data['cust_id'] == x, 'cltv']) > str(new_data.loc[old_data['cust_id'] == x, 'cltv']):
            counter_1 = counter_1 + 1
        elif str(old_data.loc[old_data['cust_id'] == x, 'cltv']) < str(new_data.loc[old_data['cust_id'] == x, 'cltv']):
            counter_2 = counter_2 + 1
    Nutshell_2 = "{value1} CUSTOMERS saw CLTV DECREASED yesterday which is {value2}% of the total base"    .format(value1 = counter_1, value2 = int(round((counter_1 / len(new_data)) *100)))
    Nutshell_3 = "{value1} CUSTOMERS saw CLTV INCREASED yesterday which is {value2}% of the total base"    .format(value1 = counter_2, value2 = int(round((counter_2 / len(new_data)) *100)))
    churned_old = old_data[['churn', 'custom_date_identifier']]
    old_churn_count = churned_old.groupby(['custom_date_identifier', 'churn']).size().reset_index()
    old_churn_count.rename(columns={0:'counter'}, inplace=True)
    churned_new = new_data[['custom_date_identifier','churn',]]
    new_churn_count = churned_new.groupby(['custom_date_identifier', 'churn']).size().reset_index()
    new_churn_count.rename(columns={0:'counter'}, inplace=True)
    value1 = new_churn_count.counter.iloc[-1] - old_churn_count.counter.iloc[-2]
    value2 = int(round((new_churn_count.counter.iloc[-1] - old_churn_count.counter.iloc[-1]) / len(new_data.query('churn == 1')) * 100))
    value3 = "High" if len(old_data.query('churn == 1')) <= len(new_data.query('churn == 1')) else "Low"
    Nutshell_4 = "{value1} CUSTOMERS CHURNED yesterday which is {value2}% {value3} compared to previous day"    .format(value1 = value1,value2 = value2, value3 = value3)
    value_cust_old = old_data[['cust_id', 'value', 'custom_date_identifier']]
    cust_id_low = value_cust_old.loc[value_cust_old['value'] == "Low", 'cust_id'].reset_index(drop = True)
    cust_id_low = cust_id_low.to_frame()
    value_cust_new = new_data[['cust_id', 'value', 'custom_date_identifier']]
    value_cust_new_changed = pd.merge(value_cust_new, cust_id_low, on='cust_id', how='right')
    value_changed = value_cust_new_changed.groupby(['custom_date_identifier', 'value']).size().reset_index()
    value_changed.rename(columns={0:'counter'}, inplace=True)
    high_count = value_changed.loc[value_changed['value'] == "High", 'counter'].get_values()
    very_high_count = value_changed.loc[value_changed['value'] == "Very High", 'counter'].get_values()
    Nutshell_5 = "{value1} CUSTOMERS saw Value Segment changed to Low to High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(high_count) == 0 else int(high_count),             value2 = int(round((0 if len(high_count) == 0 else int(high_count) / len(old_data.query('value == "Low"')) * 100))))
    Nutshell_6 = "{value1} CUSTOMERS saw Value Segment changed to Low to Very High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_high_count) == 0 else int(very_high_count),             value2 = int(round((0 if len(very_high_count) == 0 else int(very_high_count) / len(old_data.query('value == "Low"')) * 100))))
    cust_id_very_low = value_cust_old.loc[value_cust_old['value'] == "Very Low", 'cust_id'].reset_index(drop = True)
    cust_id_very_low = cust_id_very_low.to_frame()
    value_cust_new_changed_1 = pd.merge(value_cust_new, cust_id_very_low, on='cust_id', how='right')
    value_changed_1 = value_cust_new_changed_1.groupby(['custom_date_identifier', 'value']).size().reset_index()
    value_changed_1.rename(columns={0:'counter'}, inplace=True)
    very_high_count_1 = value_changed_1.loc[value_changed_1['value'] == "Very High", 'counter'].get_values()
    Nutshell_7 = "{value1} CUSTOMERS saw Value Segment changed to Very Low to Very High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_high_count_1) == 0 else int(very_high_count_1),             value2 = int(round((0 if len(very_high_count_1) == 0 else int(very_high_count_1) / len(old_data.query('value == "Very Low"')) * 100))))
    cust_id_high = value_cust_old.loc[value_cust_old['value'] == "High", 'cust_id'].reset_index(drop = True)
    cust_id_high = cust_id_high.to_frame()
    value_cust_new_changed_2 = pd.merge(value_cust_new, cust_id_high, on='cust_id', how='right')
    value_changed_2 = value_cust_new_changed_2.groupby(['custom_date_identifier', 'value']).size().reset_index()
    value_changed_2.rename(columns={0:'counter'}, inplace=True)
    low_count_1 = value_changed_2.loc[value_changed_2['value'] == "Low", 'counter'].get_values()
    Nutshell_8 = "{value1} CUSTOMERS saw Value Segment changed to High to Low yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(low_count_1) == 0 else int(low_count_1), value2 = int(round((0 if len(low_count_1) == 0 else int(low_count_1) / len(old_data.query('value == "High"')) * 100))))
    cust_id_very_high = value_cust_old.loc[value_cust_old['value'] == "Very High", 'cust_id'].reset_index(drop = True)
    cust_id_very_high = cust_id_very_high.to_frame()
    value_cust_new_changed_3 = pd.merge(value_cust_new, cust_id_very_high, on='cust_id', how='right')
    value_changed_3 = value_cust_new_changed_3.groupby(['custom_date_identifier', 'value']).size().reset_index()
    value_changed_3.rename(columns={0:'counter'}, inplace=True)
    low_count_2 = value_changed_3.loc[value_changed_3['value'] == "Low", 'counter'].get_values()
    very_low_count = value_changed_3.loc[value_changed_3['value'] == "Very Low", 'counter'].get_values()
    Nutshell_9 = "{value1} CUSTOMERS saw Value Segment changed to Very High to Low yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(low_count_2) == 0 else int(low_count_2),             value2 = int(round((0 if len(low_count_2) == 0 else int(low_count_2) / len(old_data.query('value == "Very High"')) * 100))))
    Nutshell_10 = "{value1} CUSTOMERS saw Value Segment changed to Very High to Very Low yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_low_count) == 0 else int(very_low_count),             value2 = int(round((0 if len(very_low_count) == 0 else int(very_low_count) / len(old_data.query('value == "Very High"')) * 100))))
    eng_cust_old = old_data[['cust_id', 'engagement', 'custom_date_identifier']]
    eng_cust_new = new_data[['cust_id', 'engagement', 'custom_date_identifier']]
    cust_id_eng_low = eng_cust_old.loc[eng_cust_old['engagement'] == "Low", 'cust_id'].reset_index(drop = True)
    cust_id_eng_low = cust_id_eng_low.to_frame()
    eng_cust_new_changed = pd.merge(eng_cust_new, cust_id_eng_low, on='cust_id', how='right')
    eng_changed = eng_cust_new_changed.groupby(['custom_date_identifier', 'engagement']).size().reset_index()
    eng_changed.rename(columns={0:'counter'}, inplace=True)
    high_count_eng = eng_changed.loc[eng_changed['engagement'] == "High", 'counter'].get_values()
    very_high_count_eng = eng_changed.loc[eng_changed['engagement'] == "Very High", 'counter'].get_values()
    Nutshell_11 = "{value1} CUSTOMERS saw Engagement Segment changed to Low to High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(high_count_eng) == 0 else int(high_count_eng),             value2 = int(round((0 if len(high_count_eng) == 0 else int(high_count_eng) / len(old_data.query('engagement == "Low"')) * 100))))
    Nutshell_12 = "{value1} CUSTOMERS saw Engagement Segment changed to Low to Very High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_high_count_eng) == 0 else int(very_high_count_eng),             value2 = int(round((0 if len(very_high_count_eng) == 0 else int(very_high_count_eng) / len(old_data.query('engagement == "Low"')) * 100))))
    cust_id_eng_very_low = eng_cust_old.loc[eng_cust_old['engagement'] == "Very Low", 'cust_id'].reset_index(drop = True)
    cust_id_eng_very_low = cust_id_eng_very_low.to_frame()
    eng_cust_new_changed_1 = pd.merge(eng_cust_new, cust_id_eng_very_low, on='cust_id', how='right')
    eng_changed_1 = eng_cust_new_changed_1.groupby(['custom_date_identifier', 'engagement']).size().reset_index()
    eng_changed_1.rename(columns={0:'counter'}, inplace=True)
    very_high_count_eng_1 = eng_changed_1.loc[eng_changed_1['engagement'] == "Very High", 'counter'].get_values()
    Nutshell_13 = "{value1} CUSTOMERS saw Engagement Segment changed to Very Low to Very High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_high_count_eng_1) == 0 else int(very_high_count_eng_1),             value2 = int(round((0 if len(very_high_count_eng_1) == 0 else int(very_high_count_eng_1) / len(old_data.query('engagement == "Very Low"')) * 100))))
    cust_id_eng_high = eng_cust_old.loc[eng_cust_old['engagement'] == "High", 'cust_id'].reset_index(drop = True)
    cust_id_eng_high = cust_id_eng_high.to_frame()
    eng_cust_new_changed_2 = pd.merge(eng_cust_new, cust_id_eng_high, on='cust_id', how='right')
    eng_changed_2 = eng_cust_new_changed_2.groupby(['custom_date_identifier', 'engagement']).size().reset_index()
    eng_changed_2.rename(columns={0:'counter'}, inplace=True)
    low_count_eng = eng_changed_2.loc[eng_changed_2['engagement'] == "Low", 'counter'].get_values()
    Nutshell_14 = "{value1} CUSTOMERS saw Engagement Segment changed to Very Low to Very High yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(low_count_eng) == 0 else int(low_count_eng),             value2 = int(round((0 if len(low_count_eng) == 0 else int(low_count_eng) / len(old_data.query('engagement == "High"')) * 100))))
    cust_id_eng_very_high = eng_cust_old.loc[eng_cust_old['engagement'] == "Very High", 'cust_id'].reset_index(drop = True)
    cust_id_eng_very_high = cust_id_eng_very_high.to_frame()
    eng_cust_new_changed_3 = pd.merge(eng_cust_new, cust_id_eng_very_high, on='cust_id', how='right')
    eng_changed_3 = eng_cust_new_changed_3.groupby(['custom_date_identifier', 'engagement']).size().reset_index()
    eng_changed_3.rename(columns={0:'counter'}, inplace=True)
    low_count_eng_1 = eng_changed_3.loc[eng_changed_3['engagement'] == "Low", 'counter'].get_values()
    very_low_count_eng = eng_changed_3.loc[eng_changed_3['engagement'] == "Very Low", 'counter'].get_values()
    Nutshell_15 = "{value1} CUSTOMERS saw Engagement Segment changed to Very High to Low yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(low_count_eng_1) == 0 else int(low_count_eng_1),             value2 = int(round((0 if len(low_count_eng_1) == 0 else int(low_count_eng_1) / len(old_data.query('engagement == "Very High"')) * 100))))
    Nutshell_16 = "{value1} CUSTOMERS saw Engagement Segment changed to Very High to Very Low yesterday which is {value2}% of the total base"    .format(value1 = 0 if len(very_low_count_eng) == 0 else int(very_low_count_eng),             value2 = int(round((0 if len(very_low_count_eng) == 0 else int(very_low_count_eng) / len(old_data.query('engagement == "Very High"')) * 100))))
    Nutshell_17 = "Currently {value1} Customers have Very Low Value but Very High Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Very Low" and engagement == "Very High"')),            value2 = int(round((len(new_data.query('value == "Very Low" and engagement == "Very High"')) / len(new_data)) * 100)))
    Nutshell_18 = "Currently {value1} Customers have Very Low Value but High Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Very Low" and engagement == "High"')),            value2 = int(round((len(new_data.query('value == "Very Low" and engagement == "High"')) / len(new_data)) * 100)))
    Nutshell_19 = "Currently {value1} Customers have Very High Value but Low Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Very High" and engagement == "Low"')),            value2 = int(round((len(new_data.query('value == "Very High" and engagement == "Low"')) / len(new_data)) * 100)))
    Nutshell_20 = "Currently {value1} Customers have Very High Value but Very Low Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Very High" and engagement == "Very Low"')),            value2 = int(round((len(new_data.query('value == "Very High" and engagement == "Very Low"')) / len(new_data)) * 100)))
    Nutshell_21 = "Currently {value1} Customers have High Value but Low Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "High" and engagement == "Low"')),            value2 = int(round((len(new_data.query('value == "High" and engagement == "Low"')) / len(new_data)) * 100)))
    Nutshell_22 = "Currently {value1} Customers have High Value but Very Low Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "High" and engagement == "Very Low"')),            value2 = int(round((len(new_data.query('value == "High" and engagement == "Very Low"')) / len(new_data)) * 100)))
    Nutshell_23 = "Currently {value1} Customers have Low Value but Very High Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Low" and engagement == "Very High"')),            value2 = int(round((len(new_data.query('value == "Low" and engagement == "Very High"')) / len(new_data)) * 100)))
    Nutshell_24 = "Currently {value1} Customers have Low Value but High Engagement which is {value2}% of total base"    .format(value1 = len(new_data.query('value == "Low" and engagement == "High"')),            value2 = int(round((len(new_data.query('value == "Low" and engagement == "High"')) / len(new_data)) * 100)))
    nutshell_dict = {'command':['Nutshell_1','Nutshell_2','Nutshell_3','Nutshell_4','Nutshell_5',
                                'Nutshell_6','Nutshell_7','Nutshell_8','Nutshell_9','Nutshell_10',
                                'Nutshell_11','Nutshell_12','Nutshell_13','Nutshell_14','Nutshell_15',
                                'Nutshell_16','Nutshell_17','Nutshell_18','Nutshell_19','Nutshell_20',
                                'Nutshell_21','Nutshell_22','Nutshell_23','Nutshell_24'],
                     'description':[Nutshell_1,Nutshell_2,Nutshell_3,Nutshell_4,Nutshell_5,
                                    Nutshell_6,Nutshell_7,Nutshell_8,Nutshell_9,Nutshell_10,
                                    Nutshell_11,Nutshell_12,Nutshell_13,Nutshell_14,Nutshell_15,
                                    Nutshell_16,Nutshell_17,Nutshell_18,Nutshell_19,Nutshell_20,
                                    Nutshell_21,Nutshell_22,Nutshell_23,Nutshell_24]}
    nutshell_df = pd.DataFrame(nutshell_dict)
    nutshell_df['cltv'] = ""
    nutshell_df['churn'] = ""
    nutshell_df['value'] = ""
    nutshell_df['engagement'] = ""
    for x in range(0, len(nutshell_df)):
        if 'CLTV' in nutshell_df.description[x]:
            nutshell_df.cltv[x] = 1
        else:
            nutshell_df.cltv[x] = 0
        if 'CHURNED' in nutshell_df.description[x]:
            nutshell_df.churn[x] = 1
        else:
            nutshell_df.churn[x] = 0
        if 'Value' in nutshell_df.description[x]:
            nutshell_df.value[x] = 1
        else:
            nutshell_df.value[x] = 0
        if 'Engagement' in nutshell_df.description[x]:
            nutshell_df.engagement[x] = 1
        else:
            nutshell_df.engagement[x] = 0
    engine = create_engine('')
    nutshell_df.to_sql('nutshell', engine, if_exists='replace', index=False)
except Exception as e:
    print e
    temp.conn.rollback()
