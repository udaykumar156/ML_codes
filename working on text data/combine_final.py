import pandas as pd
import numpy as np



mma = pd.read_csv('mma_no_nsr_int.csv')
internal = pd.read_csv('internaldata_more_info.csv') 
doc = pd.read_csv('doc_more_info.csv')
nsr = pd.read_csv('nsr_more_info.csv')
kpj = pd.read_csv('kpj.csv')
regency = pd.read_csv('regency.csv')
island = pd.read_csv('island.csv')
pc = pd.read_csv('pcsh.csv')



print "MMA       -->",mma.shape
print "DocDoc    -->",doc.shape
print "NSR       -->",nsr.shape
print "Internal  -->",internal.shape
print "KPJ       -->",kpj.shape
print "Regency   -->",regency.shape
print "Island    -->",island.shape
print "PCSH      -->",pc.shape



mma.columns, internal.columns, doc.columns, nsr.columns

combine1 = pd.concat([mma, internal,doc,nsr, kpj, regency, island, pc])

combine1 = combine1[['name','mmcreg_no','nsr_no','gender','city','state','speciality_1','speciality_2','education',
                   'affiliated_hospital','clinics','languages','tel_no','fax_no','email','suminsured','premium',
                   'category','memberships','status','insurance','date_full_registration','provisional_regno',
                   'date_provisional','graduated_college','id','last_practise','experience','credentials','certification',
                   'doc_data','nsr_data','internal_data','mma_data','kpj_data','regency_data','island_data','pcsh_data',
                   'undergraduate_college', 'academic_affiliation','focus','work_days','awards','consultant_type',
                   'qualifications','clinical_practise','procedures','overview','apc']]



combine1.reset_index(drop=True, inplace=True)
combine1.state = combine1.state.str.strip(' ')
len(combine1.state.unique())
combine1.state = combine1.state.str.replace('WILAYAH PERSEKUTUAN KUALA LUMPUR', 'KUALA LUMPUR')
combine1.state = combine1.state.str.replace('SELANGOR DARUL EHSAN', 'SELANGOR')
combine1.state = combine1.state.str.replace('WILAYAH PERSEKUTUANPUTRAJAYA', 'PUTRAJAYA')
combine1.state = combine1.state.str.replace('WILAYAH PERSEKUTUANKUALA LUMPUR', 'KUALA LUMPUR')
combine1.state = combine1.state.str.replace('WILAYAH PERSEKUTUAN PUTRAJAYA', 'PUTRAJAYA')
len(combine1.state.unique())
combine1.state.unique()
combine1.mma_data.fillna(0, inplace=True)
combine1.doc_data.fillna(0, inplace=True)
combine1.nsr_data.fillna(0, inplace=True)
combine1.internal_data.fillna(0, inplace=True)
combine1.kpj_data.fillna(0, inplace=True)
combine1.regency_data.fillna(0, inplace=True)
combine1.island_data.fillna(0, inplace=True)
combine1.pcsh_data.fillna(0, inplace=True)
combine1.loc[combine1.internal_data == 1]
combine1.loc[combine1.name.duplicated()]

combine1.name = combine1.name.str.upper()
combine1['duplicated_name'] = combine1.name.str.replace(' ','')
combine_final_data = (combine1.groupby(['duplicated_name'], as_index=False).apply(lambda x: x if len(x)==1 else x.iloc[[-1]])
         .reset_index(level=0, drop=True))



combine_final_data.loc[combine_final_data.duplicated_name.duplicated()]
combine_final_data.reset_index(drop=True, inplace=True)
combine_final_data.name = combine_final_data.name.str.upper()
combine_final_data['duplicated_name'] = combine_final_data.name.str.replace(' ','')

names = pd.read_csv('/home/ubuntu/data_analysis/mma/negative_mma/negative_names.csv')
names['names_dupli'] = names.name.str.replace(' ','')
negative_names = combine_final_data[combine_final_data.duplicated_name.isin(names.names_dupli)]

negtive_index = negative_names.duplicated_name.index
combine_final_data['negative_data'] = ""
combine_final_data.negative_data.ix[negtive_index] = 1
combine_final_data.negative_data.replace('', 0, inplace=True)


combine1.name = combine1.name.str.upper()
combine1['duplicated_name'] = combine1.name.str.replace(' ','')
combine1.loc[combine1.duplicated_name.duplicated()]
combine_final = combine1.drop_duplicates(subset = ['duplicated_name'])
combine_final.loc[combine_final.regency_data ==1]
combine_final.reset_index(drop=True, inplace=True)

negative_names = combine_final[combine_final.duplicated_name.isin(names.names_dupli)]
negtive_index = negative_names.duplicated_name.index
combine_final['negative_data'] = ""
combine_final.negative_data.ix[negtive_index] = 1
combine_final.negative_data.replace('', 0, inplace=True)
