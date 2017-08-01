import pandas as pd
import numpy as np
from sqlalchemy import create_engine

engine = create_engine('###################################')
island = pd.read_sql_table(table_name='islandhospital_final', con=engine)

island.rename(columns={'subspecialty':'speciality_2'}, inplace=True)
island.rename(columns={'language':'languages'}, inplace=True)
island.rename(columns={'qualifications':'education'}, inplace=True)
island.rename(columns={'specialty':'speciality_1'}, inplace=True)
island.rename(columns={'mmc_reg':'mmcreg_no'}, inplace=True)
island.rename(columns={'awards / achievements / appointments':'awards'}, inplace=True)

island.drop(['image','link','location','id'], axis=1, inplace=True)

island.speciality_1 = island.speciality_1.str.strip('[]')
island.speciality_2 = island.speciality_2.str.strip('[]')
island.languages = island.languages.str.strip('[]')
island.education = island.education.str.strip('[]')
island.focus = island.focus.str.strip('[]')
island.awards = island.awards.str.strip('[]')

island.speciality_1 = island.speciality_1.str.strip("''")
island.speciality_1 = island.speciality_1.str.replace("'",'')
island.speciality_1 = island.speciality_1.str.upper()
island.speciality_2 = island.speciality_2.str.strip("''")
island.speciality_2 = island.speciality_2.str.replace("'","")
island.speciality_2 = island.speciality_2.str.upper()
island.mmcreg_no[0]
island.mmcreg_no = island.mmcreg_no.str.replace('MMC Registration No: ', '')

island['duplicated_name'] = island.name.str.replace("Dr.","")
island.duplicated_name = island.duplicated_name.str.replace("Dato'", '')
island.duplicated_name = island.duplicated_name.str.replace("Assoc. Prof. ",'')
island.duplicated_name = island.duplicated_name.str.replace("Prof. ", "")
island.duplicated_name = island.duplicated_name.str.replace("Datin ", "")
island.duplicated_name = island.duplicated_name.str.replace("Datin","")
island.duplicated_name = island.duplicated_name.str.replace("Mr.","")
island.duplicated_name = island.duplicated_name.str.replace(".","")
island.duplicated_name = island.duplicated_name.str.replace(" ","")
island.duplicated_name = island.duplicated_name.str.upper()


island = island[['name','mmcreg_no','speciality_1','speciality_2','education','languages','focus','awards','duplicated_name']]
island.mmcreg_no = island.mmcreg_no.str.replace(' ','')
island['island_data'] = 1
island.reset_index(drop=True, inplace=True)
island.to_csv('island_final.csv', index=False,mode='w')


other = pd.read_csv('mma_remaining.csv')
island_more_info = pd.merge(island,other,on=['mmcreg_no'])
island_more_info.isnull().sum()
island_more_info.drop(['island_data','name_y','education_y','id','mma_data'], axis=1, inplace=True)
island_more_info.rename(columns={'name_x':'name'},inplace=True)
island_more_info.rename(columns={'education_x':'education'},inplace=True)

island_remaining =  island[~island.mmcreg_no.isin(island_more_info.mmcreg_no)]
island_mma = pd.concat([island_more_info,island_remaining])
island_mma.to_csv('island.csv', index=False, mode='w')

