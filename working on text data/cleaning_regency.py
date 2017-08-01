import pandas as pd
import numpy as np
from sqlalchemy import create_engine
engine = create_engine('#######################################')
reg = pd.read_sql_table(table_name='regency_final', con=engine)
reg.fillna(np.nan, inplace=True)
reg.replace('None', np.nan, inplace=True)
reg.replace(' ', np.nan, inplace=True)
reg.isnull().sum()
reg.drop(['other degree','special interests','website',], axis=1, inplace=True)
reg.rename(columns={'consultation hour':'work_days'}, inplace=True)
reg.rename(columns={'mmc full reg. no':'mmcreg_no'}, inplace=True)
reg.rename(columns={'medical education':'education'}, inplace=True)
reg.rename(columns={'professional membership':'memberships'}, inplace=True)
reg.rename(columns={'procedure specialties':'speciality_1'}, inplace=True)
reg.rename(columns={'designation':'speciality_2'}, inplace=True)
reg.rename(columns={'academic affiliation':'academic_affiliation'}, inplace=True)
reg.loc[reg.academic_affiliation.str.contains('|'.join(['lecturer','Professor','Faculty']),case=False,na=False), 'parttime_prof'] = 1

reg.loc[reg.speciality_2.str.contains('Resident Consultant', case=False, na=False), 'resident_cons'] = 1
reg.loc[reg.speciality_2.str.contains('Part time Consultant', case=False, na=False), 'partime_cons'] = 1
reg.loc[reg.speciality_2.str.contains('Consultant Emergency', case=False, na=False), 'emergency_cons'] = 1
reg.loc[reg.speciality_2.str.contains('Visiting Consultant', case=False, na=False), 'visiting_cons'] = 1

reg.loc[reg.work_days.str.contains('Sunday', case=False,na=False), 'weekend'] = 1
reg.loc[reg.work_days.str.contains('By appointment', case=False, na=False), 'appointment_cons'] = 1
reg.loc[reg.work_days.str.contains('24/7', na=False), 'fullday_service'] = 1
reg.credentials.replace(np.nan, 'None', inplace=True)

import re
for x in range(len(reg)):
    if reg.credentials[x] is not None:
        reg.credentials[x] = re.sub(r'\([^\(]*?\)', r'', reg.credentials[x])


reg.education.replace(np.nan, 'None', inplace=True)

import re
for x in range(len(reg)):
    if reg.education[x] is not None:
        reg.education[x] = re.sub(r'\([^\(]*?\)', r'', reg.education[x])


reg.name = reg.name.str.split('|').str[0]


reg['duplicated_name'] = reg.name.str.strip('Dr.')
reg['duplicated_name'] = reg.duplicated_name.str.replace('S/O','')
reg['duplicated_name'] = reg.duplicated_name.str.replace('@','')
reg['duplicated_name'] = reg.duplicated_name.str.replace('A/L','')
reg['duplicated_name'] = reg.duplicated_name.str.replace('A/P','')
reg['duplicated_name'] = reg.duplicated_name.str.replace('.','')
reg['duplicated_name'] = reg.duplicated_name.str.replace(' ','')
reg.duplicated_name = reg.duplicated_name.str.upper()
reg['regency_data'] = 1
reg.reset_index(drop=True, inplace=True)
reg.shape
reg.to_csv('regency_final.csv', index=False, mode='w', encoding='utf-8')
