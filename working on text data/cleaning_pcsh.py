import pandas as pd
import numpy as np
from sqlalchemy import create_engine
engine = create_engine('#########################################')
pc = pd.read_sql_table(table_name='pcsh_final', con=engine)

pc.rename(columns={'category':'speciality_1'}, inplace=True)
pc.rename(columns={'about':'overview'}, inplace=True)
pc.rename(columns={'degree':'education'}, inplace=True)
pc.rename(columns={'contact':'tel_no'}, inplace=True)
pc.rename(columns={'consultation_hours':'work_days'}, inplace=True)

import re
for x in range(len(pc)):
    pc.education[x] = re.sub(r'\([^\(]*?\)', r'',pc.education[x])
pc.tel_no = pc.tel_no.str.replace('Direct Line : ','')
pc['duplicated_name'] = pc.name.str.replace('Dr.','')
pc['duplicated_name'] = pc.duplicated_name.str.replace("Dato'",'')
pc['duplicated_name'] = pc.duplicated_name.str.replace(".",'')
pc['duplicated_name'] = pc.duplicated_name.str.replace(" ", "")
pc['duplicated_name'] = pc.duplicated_name.str.replace("A/K", "")
pc['duplicated_name'] = pc.duplicated_name.str.replace("A/P", "")
pc['duplicated_name'] = pc.duplicated_name.str.replace("-", "")
pc['duplicated_name'] = pc.duplicated_name.str.upper()

pc.loc[pc.overview.str.contains('|'.join(['her','She']), case=True, na=False), 'gender'] = 'Female'
pc.loc[pc.overview.str.contains('|'.join(['his','He']), case=True, na=True), 'gender'] = 'Male'

pc = pc[['name','gender','speciality_1','education','consultant_type','tel_no','work_days','overview','duplicated_name']]
pc.loc[pc.consultant_type == 'Resident Consultants', 'resident_cons'] = 1
pc.loc[pc.consultant_type == 'Sessional Consultants', 'sessional_cons'] = 1
pc.loc[pc.consultant_type == 'Visiting Consultants', 'visiting_cons'] = 1
pc['pcsh_data'] = 1
pc.reset_index(drop=True, inplace=True)
pc.to_csv('pcsh_final.csv', mode='w',index=False,encoding='utf-8')




































