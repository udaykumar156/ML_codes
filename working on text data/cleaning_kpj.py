import pandas as pd
from sqlalchemy import create_engine
import numpy as np
engine = create_engine('###################################')

kpj = pd.read_sql_table(table_name='kpj_final', con=engine)

kpj.rename(columns={'office':'affiliated_hospital'}, inplace=True)
kpj.rename(columns={'other info':'overview'}, inplace=True)
kpj.rename(columns={'sub speciality':'speciality_2'}, inplace=True)
kpj.rename(columns={'work days':'work_days'}, inplace=True)
kpj.rename(columns={'degrees':'education'}, inplace=True)
kpj.rename(columns={'speciality':'speciality_1'}, inplace=True)

kpj.education = kpj.education.str.strip("[]")
kpj.education = kpj.education.str.replace("'","")

import re
for x in range(len(kpj)):
    kpj.education[x] = re.sub(r'\([^\(]*?\)', r'', kpj.education[x])


kpj.affiliated_hospital = kpj.affiliated_hospital.str.strip("[]")
kpj.affiliated_hospital = 'KPJ Ipoh Specialist Hospital'
kpj.work_days = kpj.work_days.str.strip("[]")
print kpj.work_days[0]
kpj.loc[kpj.work_days.str.contains('|'.join(["Sunday"]),case=False) == True, 'weekend'] = 1
kpj.replace('', np.nan, inplace=True)
kpj.replace('None', np.nan, inplace=True)

kpj['duplicated_name'] = kpj.name.str.strip("Dr.")
kpj['duplicated_name'] = kpj.duplicated_name.str.strip("ato' Dr.")
kpj['duplicated_name'] = kpj.duplicated_name.str.replace(" @ ", '')
kpj['duplicated_name'] = kpj.duplicated_name.str.replace('.','')
kpj['duplicated_name'] = kpj.duplicated_name.str.replace('a/l','')
kpj['duplicated_name'] = kpj.duplicated_name.str.replace(' ', '')
kpj['duplicated_name'] = kpj.duplicated_name.str.upper()

kpj.reset_index(drop=True, inplace=True)

[kpj.overview.str.contains('her', case=False, na=False), 'gender'] = 'Female'
[kpj.overview.str.contains('his', case=False, na=True), 'gender'] = 'Male'

kpj['kpj_data'] = 1
kpj.reset_index(drop=True, inplace=True)

kpj.to_csv('kpj_final.csv', index=False, mode='w', encoding='utf-8')
kpj.loc[kpj.weekend == 1]

