import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')
engine = create_engine('574y698u32434069823540i3209b5n834095umv32oij34iotu4')

dent = pd.read_sql_table(table_name='dentists_nearby_final', con=engine)
dent.drop(['username','contact-state','contact-country','id','contact-webpage','contact-street','contact-emailto','status','contact-postcode'], axis=1, inplace=True)

dent.rename(columns={'name':'affiliated_hospital'}, inplace=True)
dent.rename(columns={'contact-telephone':'tel_no'}, inplace=True)
dent.rename(columns={'contact-suburb':'address'}, inplace=True)
dent.rename(columns={'degrees':'education'}, inplace=True)
dent.rename(columns={'consultation hours':'work_days'}, inplace=True)
dent.rename(columns={'attending doctors':'name'}, inplace=True)
dent.rename(columns={'contact-fax':'fax_no'}, inplace=True)
dent.rename(columns={'contact-mobile':'mobile_no'}, inplace=True)
dent.rename(columns={'category':'dentist_category'}, inplace=True)

dent_doctors = dent.loc[dent.name.str.contains('Dr.', na=False)]
dent_doctors.reset_index(drop=True, inplace=True)
dent_doctors.rename(columns={'name':'name_edu'}, inplace=True)
dent_doctors['name'] = ""
dent_doctors['name'] = dent_doctors.name_edu.str.split('-').str[0]
dent_doctors.education = dent_doctors.name_edu.str.split('-').str[1]
dent_doctors.education.replace(np.nan,'None', inplace=True)

import re
for x in range(len(dent_doctors)):
    if dent_doctors.education[x] is not None:
        dent_doctors.education[x] = re.sub(r'\([^\(]*?\)', r'', dent_doctors.education[x])

for x in range(len(dent_doctors)):
    dent_doctors.education[x] = re.sub(r'\d+', '', dent_doctors.education[x])

states = ['Selangor','Kuala Lumpur','Sarawak','Johor','Sabah', 'Penang','Perak','Pahang','Negeri Sembilan','Kedah','Melaka',
         'Terengganu','Kelantan','Labuan', 'Perlis']

dent_doctors.loc[dent_doctors.address.str.contains(states[0], na=True, case=False), 'state'] = 'SELANGOR'
dent_doctors.loc[dent_doctors.address.str.contains(states[1], na=True, case=False), 'state'] = 'KUALA LUMPUR'
dent_doctors.loc[dent_doctors.address.str.contains(states[2], na=True, case=False), 'state'] = 'SARAWAK'
dent_doctors.loc[dent_doctors.address.str.contains(states[3], na=True, case=False), 'state'] = 'JOHOR'
dent_doctors.loc[dent_doctors.address.str.contains(states[4], na=True, case=False), 'state'] = 'SABAH'
dent_doctors.loc[dent_doctors.address.str.contains(states[5], na=True, case=False), 'state'] = 'PENANG'
dent_doctors.loc[dent_doctors.address.str.contains(states[6], na=True, case=False), 'state'] = 'PERAK'
dent_doctors.loc[dent_doctors.address.str.contains(states[7], na=True, case=False), 'state'] = 'PAHANG'
dent_doctors.loc[dent_doctors.address.str.contains(states[8], na=True, case=False), 'state'] = 'NEGERI SEMBILAN'
dent_doctors.loc[dent_doctors.address.str.contains(states[9], na=True, case=False), 'state'] = 'KEDAH'
dent_doctors.loc[dent_doctors.address.str.contains(states[10], na=True, case=False), 'state'] = 'MELAKA'
dent_doctors.loc[dent_doctors.address.str.contains(states[11], na=True, case=False), 'state'] = 'TERENGGANU'
dent_doctors.loc[dent_doctors.address.str.contains(states[12], na=True, case=False), 'state'] = 'KELANTAN'
dent_doctors.loc[dent_doctors.address.str.contains(states[13], na=True, case=False), 'state'] = 'LABUAN'
dent_doctors.loc[dent_doctors.address.str.contains(states[14], na=True, case=False), 'state'] = 'PERLIS'

cities = ['Kuala Lumpur','George Town','Ipoh','Shah Alam', 'Petaling Jaya','Johor Bahru','Melaka',
          'Kota Kinabalu','Alor Setar','Kuala Terengganu']

dent_doctors.loc[dent_doctors.address.str.contains(cities[0], na=True, case=False), 'city'] = 'KUALA LUMPUR'
dent_doctors.loc[dent_doctors.address.str.contains(cities[1], na=True, case=False), 'city'] = 'GEORGE TOWN'
dent_doctors.loc[dent_doctors.address.str.contains(cities[2], na=True, case=False), 'city'] = 'IPOH'
dent_doctors.loc[dent_doctors.address.str.contains(cities[3], na=True, case=False), 'city'] = 'SHAH ALAM'
dent_doctors.loc[dent_doctors.address.str.contains(cities[4], na=True, case=False), 'city'] = 'PETALING JAYA'
dent_doctors.loc[dent_doctors.address.str.contains(cities[5], na=True, case=False), 'city'] = 'JOHOR BAHRU'
dent_doctors.loc[dent_doctors.address.str.contains(cities[6], na=True, case=False), 'city'] = 'MELAKA'
dent_doctors.loc[dent_doctors.address.str.contains(cities[7], na=True, case=False), 'city'] = 'KOTA KINABALU'
dent_doctors.loc[dent_doctors.address.str.contains(cities[8], na=True, case=False), 'city'] = 'ALOR SETAR'
dent_doctors.loc[dent_doctors.address.str.contains(cities[9], na=True, case=False), 'city'] = 'KUALA TERENGGANU'

dent_doctors.tags = dent_doctors.tags.str.strip('[]')
dent_doctors.tags = dent_doctors.tags.str.replace("'",'')

import re
for x in range(len(dent_doctors)):
    if dent_doctors.tags[x] is not None:
        dent_doctors.tags[x] = re.sub(r'\([^\(]*?\)', r'', dent_doctors.tags[x])

dent_doctors['duplicated_name'] = dent_doctors['name']
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.split(',').str[0]
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("Dato'", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("Datuk", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("Dr.", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace(".", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace(" ", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("A/L", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("A/P", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.replace("@", '')
dent_doctors.duplicated_name = dent_doctors.duplicated_name.str.upper()

import re
for x in range(len(dent_doctors)):
    if dent_doctors.affiliated_hospital[x] is not None:
        dent_doctors.affiliated_hospital[x] = re.sub(r'\([^\(]*?\)', r'', dent_doctors.affiliated_hospital[x])

dent_doctors['dent_data'] = 1
dent_doctors.reset_index(drop=True, inplace=True)
dent_doctors.to_csv('dentist_final.csv', index=False, mode='w', encoding='utf-8')

