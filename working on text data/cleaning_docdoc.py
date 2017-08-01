import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
from sqlalchemy import create_engine
engine = create_engine('$#TU#TTT	UY$@T&U@&I	YI@Y	#Y(*@#(*@	(#*')

docdoc_data = pd.read_sql_table(table_name='docdoc',con=engine)

docdoc_data.rename(columns={'languages spoken':'languages'}, inplace=True)
docdoc_data.rename(columns={'professional memberships':'memberships'}, inplace=True)
docdoc_data.rename(columns={'affiliated hospital':'affiliated_hospital'}, inplace=True)
docdoc_data.rename(columns={'fullname':'name'}, inplace=True)
docdoc_data.rename(columns={'speciality':'speciality_1'}, inplace=True)

docdoc_data = docdoc_data[['name','speciality_1','region','overview','procedures','languages','clinics','education','memberships','affiliated_hospital', 'insurance']]

states = ['Selangor','Kuala Lumpur','Sarawak','Johor','Sabah', 'Penang','Perak','Pahang','Negeri Sembilan','Kedah','Melaka',
         'Terengganu','Kelantan','Labuan', 'Perlis']

docdoc_data.loc[docdoc_data.region.str.contains(states[0], na=True, case=False), 'state'] = 'SELANGOR'
docdoc_data.loc[docdoc_data.region.str.contains(states[1], na=True, case=False), 'state'] = 'KUALA LUMPUR'
docdoc_data.loc[docdoc_data.region.str.contains(states[2], na=True, case=False), 'state'] = 'SARAWAK'
docdoc_data.loc[docdoc_data.region.str.contains(states[3], na=True, case=False), 'state'] = 'JOHOR'
docdoc_data.loc[docdoc_data.region.str.contains(states[4], na=True, case=False), 'state'] = 'SABAH'
docdoc_data.loc[docdoc_data.region.str.contains(states[5], na=True, case=False), 'state'] = 'PENANG'
docdoc_data.loc[docdoc_data.region.str.contains(states[6], na=True, case=False), 'state'] = 'PERAK'
docdoc_data.loc[docdoc_data.region.str.contains(states[7], na=True, case=False), 'state'] = 'PAHANG'
docdoc_data.loc[docdoc_data.region.str.contains(states[8], na=True, case=False), 'state'] = 'NEGERI SEMBILAN'
docdoc_data.loc[docdoc_data.region.str.contains(states[9], na=True, case=False), 'state'] = 'KEDAH'
docdoc_data.loc[docdoc_data.region.str.contains(states[10], na=True, case=False), 'state'] = 'MELAKA'
docdoc_data.loc[docdoc_data.region.str.contains(states[11], na=True, case=False), 'state'] = 'TERENGGANU'
docdoc_data.loc[docdoc_data.region.str.contains(states[12], na=True, case=False), 'state'] = 'KELANTAN'
docdoc_data.loc[docdoc_data.region.str.contains(states[13], na=True, case=False), 'state'] = 'LABUAN'
docdoc_data.loc[docdoc_data.region.str.contains(states[14], na=True, case=False), 'state'] = 'PERLIS'

docdoc_data.state.unique()

docdoc_data.clinics = docdoc_data.clinics.str.strip("[]")
docdoc_data.clinics = docdoc_data.clinics.str.strip("'")

docdoc_data.overview = docdoc_data.overview.str.strip("[]")
docdoc_data.overview = docdoc_data.overview.str.strip("u'")

docdoc_data.languages = docdoc_data.languages.str.strip("[]")
docdoc_data.languages = docdoc_data.languages.str.replace("'","")

docdoc_data.education = docdoc_data.education.str.strip('[]')
docdoc_data.education = docdoc_data.education.str.replace("'","")

docdoc_data.memberships = docdoc_data.memberships.str.strip('[]')
docdoc_data.memberships = docdoc_data.memberships.str.replace("'","")

docdoc_data.affiliated_hospital = docdoc_data.affiliated_hospital.str.strip('[]')
docdoc_data.affiliated_hospital = docdoc_data.affiliated_hospital.str.replace("'","")

docdoc_data.loc[docdoc_data.overview.str.contains('|'.join(['her','She']), case=True, na=False), 'gender'] = 'Female'
docdoc_data.loc[docdoc_data.overview.str.contains('|'.join(['his','He']), case=True, na=True), 'gender'] = 'Male'
docdoc_data.speciality_1 = docdoc_data.speciality_1.str.upper()
docdoc_data.state = docdoc_data.state.str.upper()

docdoc_data.overview.replace('one', 1, inplace=True)
docdoc_data.overview.replace('two', 2, inplace=True)
docdoc_data.overview.replace('three', 3, inplace=True)
docdoc_data.overview.replace('four', 4, inplace=True)
docdoc_data.overview.replace('five', 5, inplace=True)
docdoc_data.overview.replace('six', 6, inplace=True)
docdoc_data.overview.replace('seven', 7, inplace=True)
docdoc_data.overview.replace('eight', 8, inplace=True)
docdoc_data.overview.replace('nine', 9, inplace=True)

docdoc_data['experience'] = ""

import re
regex = re.compile('([0-9]*) years of experience')
for x in range(len(docdoc_data)):
    docdoc_data['experience'][x] = regex.findall(str(docdoc_data.overview[x]))

docdoc_data.experience = docdoc_data.experience.str[0]
docdoc_data.experience.replace(' ', np.nan, inplace=True)

docdoc_data['duplicated_name'] = docdoc_data.name.str.replace('Dr.','')
docdoc_data['duplicated_name'] = docdoc_data.duplicated_name.str.replace('Dato','')
docdoc_data['duplicated_name'] = docdoc_data.duplicated_name.str.replace('Datin','')
docdoc_data['duplicated_name'] = docdoc_data.duplicated_name.str.replace('.','')
docdoc_data['duplicated_name'] = docdoc_data.duplicated_name.str.replace(' ','')
docdoc_data['duplicated_name'] = docdoc_data.duplicated_name.str.upper()

docdoc_data.reset_index(drop=True, inplace=True)
docdoc_data['doc_data'] = 1
docdoc_data.to_csv('doc_final.csv', index=False, mode='w')

















































































