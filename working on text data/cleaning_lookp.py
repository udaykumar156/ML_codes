import pandas as pd
from sqlalchemy import create_engine
import numpy as np
engine = create_engine('#################################')

lookp = pd.read_sql_table(table_name='lookp', con=engine)

lookp.rename(columns={'overall rating':'overall_rating'}, inplace=True)
lookp.rename(columns={'value of money':'value_of_money'}, inplace=True)
lookp.rename(columns={'waiting time':'waiting_time'}, inplace=True)
lookp.rename(columns={'opening-hours':'opening_hours'}, inplace=True)
lookp.rename(columns={'profession':'speciality_1'}, inplace=True)
lookp.rename(columns={'fax':'fax_no'}, inplace=True)
lookp.rename(columns={'mobile':'mobile_no'}, inplace=True)

lookp.loc[lookp.name.duplicated()]
lookp.reset_index(drop=True, inplace=True)
lookp.tags[0]
lookp.services[0]
lookp.reviews[0]
lookp.opening_hours[1]
lookp.description[1]

list(lookp.speciality_1.unique())
len(lookp.name.unique())

lookp.loc[lookp.tags.str.contains('government clinic'), 'government_clinic'] = 1
lookp.loc[lookp.government_clinic == 1]
lookp_doctors = lookp.loc[lookp.name.str.contains('Dr.', na=False)]
lookp_doctors.reset_index(drop=True, inplace=True)
lookp_doctors.overall_rating = pd.to_numeric(lookp_doctors.overall_rating)
lookp_doctors.value_of_money = pd.to_numeric(lookp_doctors.value_of_money)
lookp_doctors.experienced = pd.to_numeric(lookp_doctors.experienced)
lookp_doctors.views = pd.to_numeric(lookp_doctors.views)
lookp_doctors.waiting_time = pd.to_numeric(lookp_doctors.waiting_time)
lookp_doctors.friendliness = pd.to_numeric(lookp_doctors.friendliness)
lookp.reset_index(drop=True, inplace=True)
government_hospitals = lookp.loc[lookp.government_clinic == 1]
government_hospitals.shape
lookp_doctors.rename(columns={'rank':'ranking'}, inplace=True)
lookp_doctors.ranking.str.extract("Rank (\d+) of (\d+)")
ranking_table = lookp_doctors.ranking.str.extract("Rank (\d+) of (\d+)", expand=True)
ranking_table.rename(columns={0:'rank_doctor'}, inplace=True)
ranking_table.rename(columns={1:'rank_total'}, inplace=True)

lookp_doctors = pd.concat([lookp_doctors, ranking_table], axis=1)

states = ['Selangor','Kuala Lumpur','Sarawak','Johor','Sabah', 'Penang','Perak','Pahang','Negeri Sembilan','Kedah','Melaka',
         'Terengganu','Kelantan','Labuan', 'Perlis']

lookp_doctors.loc[lookp_doctors.address.str.contains(states[0], na=True, case=False), 'state'] = 'SELANGOR'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[1], na=True, case=False), 'state'] = 'KUALA LUMPUR'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[2], na=True, case=False), 'state'] = 'SARAWAK'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[3], na=True, case=False), 'state'] = 'JOHOR'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[4], na=True, case=False), 'state'] = 'SABAH'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[5], na=True, case=False), 'state'] = 'PENANG'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[6], na=True, case=False), 'state'] = 'PERAK'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[7], na=True, case=False), 'state'] = 'PAHANG'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[8], na=True, case=False), 'state'] = 'NEGERI SEMBILAN'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[9], na=True, case=False), 'state'] = 'KEDAH'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[10], na=True, case=False), 'state'] = 'MELAKA'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[11], na=True, case=False), 'state'] = 'TERENGGANU'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[12], na=True, case=False), 'state'] = 'KELANTAN'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[13], na=True, case=False), 'state'] = 'LABUAN'
lookp_doctors.loc[lookp_doctors.address.str.contains(states[14], na=True, case=False), 'state'] = 'PERLIS'

lookp_doctors.state.unique()
cities = ['Kuala Lumpur','George Town','Ipoh','Shah Alam', 'Petaling Jaya','Johor Bahru','Melaka',
          'Kota Kinabalu','Alor Setar','Kuala Terengganu']

lookp_doctors.loc[lookp_doctors.address.str.contains(cities[0], na=True, case=False), 'city'] = 'KUALA LUMPUR'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[1], na=True, case=False), 'city'] = 'GEORGE TOWN'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[2], na=True, case=False), 'city'] = 'IPOH'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[3], na=True, case=False), 'city'] = 'SHAH ALAM'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[4], na=True, case=False), 'city'] = 'PETALING JAYA'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[5], na=True, case=False), 'city'] = 'JOHOR BAHRU'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[6], na=True, case=False), 'city'] = 'MELAKA'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[7], na=True, case=False), 'city'] = 'KOTA KINABALU'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[8], na=True, case=False), 'city'] = 'ALOR SETAR'
lookp_doctors.loc[lookp_doctors.address.str.contains(cities[9], na=True, case=False), 'city'] = 'KUALA TERENGGANU'


lookp_doctors.drop(['government_clinic','quality','id'], axis=1, inplace=True)
lookp_doctors.description[0]
lookp_doctors.name[0]

lookp_doctors.rename(columns={'description':'overview'}, inplace=True)
lookp_doctors.rename(columns={'phone':'tel_no'}, inplace=True)
lookp_doctors.rename(columns={'address':'hospital_address'}, inplace=True)

lookp_doctors['duplicated_name'] = lookp_doctors.name.str.replace('Klinik', '')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('Poliklinik', '')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('Professor','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('&','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('Surgeri','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.split('Dr.').str[1]

import re
for x in range(len(lookp_doctors)):
    if lookp_doctors.duplicated_name[x] is not None:
        lookp_doctors.duplicated_name[x] = re.sub(r'\([^\(]*?\)', r'', lookp_doctors.duplicated_name[x])

lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('.','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace(',','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('-','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace(' ','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('@','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('A/L','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.replace('A/P','')
lookp_doctors.duplicated_name = lookp_doctors.duplicated_name.str.upper()
lookp_doctors.opening_hours.replace('','None', inplace=True)
lookp_doctors.opening_hours[7]


lookp_doctors['sunday'] = ""
import ast
for x in range(len(lookp_doctors)):
    a = str(lookp_doctors.opening_hours[x])
    b = ast.literal_eval(a)
    if b is not None:
        lookp_doctors.sunday[x] = b['Sunday']
lookp_doctors.sunday.replace('',np.nan, inplace=True)
weekend_index = lookp_doctors.ix[lookp_doctors.sunday.notnull()].sunday.ix[lookp_doctors.sunday != '~'].index
lookp_doctors.ix[weekend_index, 'weekend'] = 1
lookp_doctors.rename(columns={'category':'clinic_category'}, inplace=True)

lookp_doctors['lookp_data'] = 1
lookp_doctors.reset_index(drop=True, inplace=True)
lookp_doctors.to_csv('lookp_final.csv', index=False, mode='w')
