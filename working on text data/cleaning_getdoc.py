import pandas as pd
from sqlalchemy import create_engine
import numpy as np
engine = create_engine('###############################################')
getdoc = pd.read_sql_table(table_name='getdoc_final', con=engine)

getdoc.replace('', np.nan, inplace=True)
getdoc.replace('None', np.nan,inplace=True)

getdoc.drop(['hospital affiliations','profile_image','id'], axis=1, inplace=True)

getdoc.rename(columns={'procedure focus':'speciality_2'}, inplace=True)
getdoc.rename(columns={'designation':'speciality_1'}, inplace=True)
getdoc.rename(columns={'procedure focus':'speciality_2'}, inplace=True)
getdoc.rename(columns={'professional statement':'overview'}, inplace=True)
getdoc.rename(columns={'fellowship/membership':'memberships'}, inplace=True)
getdoc.rename(columns={'qualifications':'education'}, inplace=True)
getdoc.rename(columns={'clinical focus':'clinical_focus'}, inplace=True)
getdoc.rename(columns={'spoken languages':'languages'}, inplace=True)

getdoc = getdoc[['name','speciality_1','speciality_2','experience','memberships','education','clinical_focus','languages','overview','clinics']]

getdoc['tel_no'] = ""

import ast
for x in range(len(getdoc)):
    a = getdoc.clinics[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'phone_number' in c.keys():
            getdoc['tel_no'][x] = c['phone_number']

getdoc['affiliated_hospital'] = ""

import ast
for x in range(len(getdoc)):
    a = getdoc.clinics[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'name' in c.keys():
            getdoc['affiliated_hospital'][x] = c['name']

getdoc['address'] = ""
import ast
for x in range(len(getdoc)):
    a = getdoc.clinics[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'address' in c.keys():
            getdoc['address'][x] = c['address']

states = ['Selangor','Kuala Lumpur','Sarawak','Johor','Sabah', 'Penang','Perak','Pahang','Negeri Sembilan','Kedah','Melaka',
         'Terengganu','Kelantan','Labuan', 'Perlis']

getdoc.loc[getdoc.address.str.contains(states[0], na=True, case=False), 'state'] = 'SELANGOR'
getdoc.loc[getdoc.address.str.contains(states[1], na=True, case=False), 'state'] = 'KUALA LUMPUR'
getdoc.loc[getdoc.address.str.contains(states[2], na=True, case=False), 'state'] = 'SARAWAK'
getdoc.loc[getdoc.address.str.contains(states[3], na=True, case=False), 'state'] = 'JOHOR'
getdoc.loc[getdoc.address.str.contains(states[4], na=True, case=False), 'state'] = 'SABAH'
getdoc.loc[getdoc.address.str.contains(states[5], na=True, case=False), 'state'] = 'PENANG'
getdoc.loc[getdoc.address.str.contains(states[6], na=True, case=False), 'state'] = 'PERAK'
getdoc.loc[getdoc.address.str.contains(states[7], na=True, case=False), 'state'] = 'PAHANG'
getdoc.loc[getdoc.address.str.contains(states[8], na=True, case=False), 'state'] = 'NEGERI SEMBILAN'
getdoc.loc[getdoc.address.str.contains(states[9], na=True, case=False), 'state'] = 'KEDAH'
getdoc.loc[getdoc.address.str.contains(states[10], na=True, case=False), 'state'] = 'MELAKA'
getdoc.loc[getdoc.address.str.contains(states[11], na=True, case=False), 'state'] = 'TERENGGANU'
getdoc.loc[getdoc.address.str.contains(states[12], na=True, case=False), 'state'] = 'KELANTAN'
getdoc.loc[getdoc.address.str.contains(states[13], na=True, case=False), 'state'] = 'LABUAN'
getdoc.loc[getdoc.address.str.contains(states[14], na=True, case=False), 'state'] = 'PERLIS'

getdoc.education.replace(np.nan, 'None', inplace=True)

import re
for x in range(len(getdoc)):
    getdoc.education[x] = re.sub(r'\([^\(]*?\)', r'', getdoc.education[x])
getdoc.experience = getdoc.experience.str.strip('years experience')

cities = ['Kuala Lumpur','George Town','Ipoh','Shah Alam', 'Petaling Jaya','Johor Bahru','Melaka',
          'Kota Kinabalu','Alor Setar','Kuala Terengganu']

getdoc.loc[getdoc.address.str.contains(cities[0], na=True, case=False), 'city'] = 'KUALA LUMPUR'
getdoc.loc[getdoc.address.str.contains(cities[1], na=True, case=False), 'city'] = 'GEORGE TOWN'
getdoc.loc[getdoc.address.str.contains(cities[2], na=True, case=False), 'city'] = 'IPOH'
getdoc.loc[getdoc.address.str.contains(cities[3], na=True, case=False), 'city'] = 'SHAH ALAM'
getdoc.loc[getdoc.address.str.contains(cities[4], na=True, case=False), 'city'] = 'PETALING JAYA'
getdoc.loc[getdoc.address.str.contains(cities[5], na=True, case=False), 'city'] = 'JOHOR BAHRU'
getdoc.loc[getdoc.address.str.contains(cities[6], na=True, case=False), 'city'] = 'MELAKA'
getdoc.loc[getdoc.address.str.contains(cities[7], na=True, case=False), 'city'] = 'KOTA KINABALU'
getdoc.loc[getdoc.address.str.contains(cities[8], na=True, case=False), 'city'] = 'ALOR SETAR'
getdoc.loc[getdoc.address.str.contains(cities[9], na=True, case=False), 'city'] = 'KUALA TERENGGANU'

getdoc['duplicated_name'] = getdoc.name.str.replace("'Dato'",'')
getdoc['duplicated_name'] = getdoc.duplicated_name.str.replace('.','')
getdoc['duplicated_name'] = getdoc.duplicated_name.str.replace(' ','')
getdoc['duplicated_name'] = getdoc.duplicated_name.str.replace('-','')
getdoc['duplicated_name'][16] = "Zulkafperi"

getdoc['getdoc_data'] = 1
getdoc.reset_index(drop=True, inplace=True)
getdoc.loc[getdoc.duplicated_name.duplicated()]
getdoc.query('name == "Yong Fee Mann"')
getdoc.drop([1888], axis=0, inplace=True)
getdoc['duplicated_name'] = getdoc.duplicated_name.str.upper()
getdoc.reset_index(drop=True, inplace=True)
getdoc.to_csv('getdoc_final.csv',index=False,mode='w',encoding='utf-8')
getdoc.shape





















