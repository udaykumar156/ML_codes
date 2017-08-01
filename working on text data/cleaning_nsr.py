import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('###############################')

nsr = pd.read_sql_table(table_name='nsr', con=engine)

nsr.rename(columns={'nsr no':'nsr_no'}, inplace=True)
nsr.rename(columns={'specialty 2':'speciality_2'}, inplace=True)
nsr.rename(columns={'specialty 1':'speciality_1'}, inplace=True)
nsr.rename(columns={'clinical practice(s)':'clinical_practise'}, inplace=True)

nsr_data = nsr[['nsr_no','name','gender','qualifications','speciality_1','speciality_2','clinical_practise','email']]

import re
for x in range(len(nsr_data)):
    nsr_data.speciality_1[x] = re.sub(r'\([^\(]*?\)', r'', nsr_data.speciality_1[x])


import re
for x in range(len(nsr_data)):
    nsr_data.speciality_2[x] = re.sub(r'\([^\(]*?\)', r'', nsr_data.speciality_2[x])

nsr_data['tel_no'] = ""

nsr.clinical_practise[0]

import ast
for x in range(len(nsr_data)):
    a = nsr_data.clinical_practise[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'Tel No' in c.keys():
            nsr_data['tel_no'][x] = c['Tel No']

# import ast
# for x in range(len(nsr_data)):
#     a = nsr_data.clinical_practise[x]
#     b = ast.literal_eval(a)
#     telArrayT = []
    
#     for c in b:
#         telArrayT.append(c['Tel No'])
#     telArray = [y for y in telArrayT if y is not None]
#     if len(telArray) > 0:
#         nsr_data['tel_no'][x] = ','.join(telArray)
#     else:
#         nsr_data['tel_no'][x] = None


nsr_data['fax_no'] = ""

import ast
for x in range(len(nsr_data)):
    a = nsr_data.clinical_practise[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'Fax No' in c.keys():
            nsr_data['fax_no'][x] = c['Fax No']

nsr_data['affiliated_hospital'] = ""
import ast
for x in range(len(nsr_data)):
    a = nsr_data.clinical_practise[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'Name' in c.keys():
            nsr_data['affiliated_hospital'][x] = c['Name']

nsr_data['hospital_address'] = ""
import ast
for x in range(len(nsr_data)):
    a = nsr_data.clinical_practise[x]
    b = ast.literal_eval(a)
    for c in b:
        if 'Address' in c.keys():
            nsr_data['hospital_address'][x] = c['Address']


nsr_data['education'] = ""
import ast
for x in range(len(nsr_data)):
    a = nsr_data.qualifications[x]
    b = ast.literal_eval(a)
    telArrayT = []
    
    for c in b:
        telArrayT.append(c['Degree/Membership/Fellowship'])
    telArray = [y for y in telArrayT if y is not None]
    if len(telArray) > 0:
        nsr_data['education'][x] = ','.join(telArray)
    else:
        nsr_data['education'][x] = None


nsr_data['award_year'] = ""
import ast
for x in range(len(nsr_data)):
    a = nsr_data.qualifications[x]
    b = ast.literal_eval(a)
    telArrayT = []
    
    for c in b:
        telArrayT.append(c['Year of award'])
    telArray = [y for y in telArrayT if y is not None]
    if len(telArray) > 0:
        nsr_data['award_year'][x] = ','.join(telArray)
    else:
        nsr_data['award_year'][x] = None

nsr_data['awarding_body'] = ""
import ast
for x in range(len(nsr_data)):
    a = nsr_data.qualifications[x]
    b = ast.literal_eval(a)
    telArrayT = []
    
    for c in b:
        telArrayT.append(c['Awarding body'])
    telArray = [y for y in telArrayT if y is not None]
    if len(telArray) > 0:
        nsr_data['awarding_body'][x] = ','.join(telArray)
    else:
        nsr_data['awarding_body'][x] = None


nsr_data.gender.value_counts()
nsr_data.query('gender == "None"')
nsr_data.gender.replace('None','Male', inplace=True)
nsr_data.gender.value_counts()

states = ['Selangor','Kuala Lumpur','Sarawak','Johor','Sabah', 'Penang','Perak','Pahang','Negeri Sembilan','Kedah','Melaka',
         'Terengganu','Kelantan','Labuan', 'Perlis']

nsr_data.loc[nsr_data.hospital_address.str.contains(states[0], na=True, case=False), 'state'] = 'SELANGOR'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[1], na=True, case=False), 'state'] = 'KUALA LUMPUR'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[2], na=True, case=False), 'state'] = 'SARAWAK'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[3], na=True, case=False), 'state'] = 'JOHOR'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[4], na=True, case=False), 'state'] = 'SABAH'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[5], na=True, case=False), 'state'] = 'PENANG'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[6], na=True, case=False), 'state'] = 'PERAK'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[7], na=True, case=False), 'state'] = 'PAHANG'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[8], na=True, case=False), 'state'] = 'NEGERI SEMBILAN'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[9], na=True, case=False), 'state'] = 'KEDAH'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[10], na=True, case=False), 'state'] = 'MELAKA'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[11], na=True, case=False), 'state'] = 'TERENGGANU'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[12], na=True, case=False), 'state'] = 'KELANTAN'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[13], na=True, case=False), 'state'] = 'LABUAN'
nsr_data.loc[nsr_data.hospital_address.str.contains(states[14], na=True, case=False), 'state'] = 'PERLIS'

cities = ['Kuala Lumpur','George Town','Ipoh','Shah Alam', 'Petaling Jaya','Johor Bahru','Melaka',
          'Kota Kinabalu','Alor Setar','Kuala Terengganu']

nsr_data.loc[nsr_data.hospital_address.str.contains(cities[0], na=True, case=False), 'city'] = 'KUALA LUMPUR'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[1], na=True, case=False), 'city'] = 'GEORGE TOWN'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[2], na=True, case=False), 'city'] = 'IPOH'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[3], na=True, case=False), 'city'] = 'SHAH ALAM'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[4], na=True, case=False), 'city'] = 'PETALING JAYA'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[5], na=True, case=False), 'city'] = 'JOHOR BAHRU'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[6], na=True, case=False), 'city'] = 'MELAKA'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[7], na=True, case=False), 'city'] = 'KOTA KINABALU'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[8], na=True, case=False), 'city'] = 'ALOR SETAR'
nsr_data.loc[nsr_data.hospital_address.str.contains(cities[9], na=True, case=False), 'city'] = 'KUALA TERENGGANU'



nsr_data['duplicated_name'] = nsr_data.name
nsr_data.duplicated_name = nsr_data.duplicated_name.str.replace('A/L','')
nsr_data.duplicated_name = nsr_data.duplicated_name.str.replace('A/P','')
nsr_data.duplicated_name = nsr_data.duplicated_name.str.replace('@','')
nsr_data.duplicated_name = nsr_data.duplicated_name.str.replace('.','')
nsr_data.duplicated_name = nsr_data.duplicated_name.str.replace(' ','')
nsr_data.duplicated_name = nsr_data.duplicated_name.str.upper()
nsr_data['nsr_data'] = 1
nsr_data.reset_index(drop=True, inplace=True)
nsr_data.shape
nsr_data.to_csv('nsr_final.csv',index=False,mode='w', encoding='utf-8')

