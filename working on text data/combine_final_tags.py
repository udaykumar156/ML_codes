import pandas as pd
import numpy as np

data = pd.read_csv('combine_apc.csv')
notnull_table = data.notnull().sum().reset_index()
notnull_table.rename(columns={0:'counter'},inplace=True )
notnull_table['contibution'] = notnull_table.counter / len(data) * 100



############TAGS#############################

data.loc[data.languages.str.contains('|'.join(["MELAYU","MALAY"]),case=False) == True, 'lang_malay'] = 1
data.lang_malay = pd.to_numeric(data.lang_malay).fillna(0)
data.loc[data.languages.isnull(), 'lang_malay'] = np.nan
data.loc[data.languages.str.contains('|'.join(["ENGLISH"]),case=False) == True, 'lang_english'] = 1
data.lang_english = pd.to_numeric(data.lang_english).fillna(0)
data.loc[data.languages.isnull(), 'lang_english'] = np.nan
data.loc[data.languages.str.contains('|'.join(["TAMIL"]),case=False) == True, 'lang_tamil'] = 1
data.lang_tamil = pd.to_numeric(data.lang_tamil).fillna(0)
data.loc[data.languages.isnull(), 'lang_tamil'] = np.nan
data.loc[data.languages.str.contains('|'.join(["BAHASA"]),case=False) == True, 'lang_bahasa'] = 1
data.lang_bahasa = pd.to_numeric(data.lang_bahasa).fillna(0)
data.loc[data.languages.isnull(), 'lang_bahasa'] = np.nan
data.loc[data.languages.str.contains('|'.join(["CHINESE","MANDARIN","CANTONESE","HOKKIEN"]),case=False) == True, 'lang_chinese'] = 1
data.lang_chinese = pd.to_numeric(data.lang_chinese).fillna(0)
data.loc[data.languages.isnull(), 'lang_chinese'] = np.nan
data.loc[data.languages.str.contains('|'.join(['HINDI','URDU']),case=False) == True, 'lang_hindi'] = 1
data.lang_hindi = pd.to_numeric(data.lang_hindi).fillna(0)
data.loc[data.languages.isnull(), 'lang_hindi'] = np.nan
data.loc[data.lang_chinese == 1]



data.loc[data.speciality_1.str.contains("FAMILY",case=False) == True, 'spe_family'] = 1
data.spe_family = pd.to_numeric(data.spe_family).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_family'] = np.nan
data.loc[data.speciality_1.str.contains("SPORTS",case=False) == True, 'spe_sports'] = 1
data.spe_sports = pd.to_numeric(data.spe_sports).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_sports'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["PAEDIATRICS","PAEDIATRICIANS"]),case=False) == True, 'spe_paediatrics'] = 1
data.spe_paediatrics = pd.to_numeric(data.spe_paediatrics).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_paediatrics'] = np.nan
data.loc[data.speciality_1.str.contains("INTERNAL MEDICINE",case=False) == True, 'spe_internal'] = 1
data.spe_internal = pd.to_numeric(data.spe_internal).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_internal'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["DERMATOLOGY","DERMATOLOGISTS"]),case=False) == True, 'spe_darmatology'] = 1
data.spe_darmatology = pd.to_numeric(data.spe_darmatology).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_darmatology'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["ENDOCRINOLOGISTS","ENDOCRINOLOGY","ENDODONTICS","ENDODONTISTS"]),case=False) == True, 'spe_endocrinology'] = 1
data.spe_endocrinology = pd.to_numeric(data.spe_endocrinology).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_endocrinology'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["ANAESTHESIOLOGISTS","ANAESTHESIOLOGY"]),case=False) == True, 'spe_anaesthesi'] = 1
data.spe_anaesthesi = pd.to_numeric(data.spe_anaesthesi).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_anaesthesi'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["UROLOGISTS","UROLOGY"]),case=False) == True, 'spe_urology'] = 1
data.spe_urology = pd.to_numeric(data.spe_urology).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_urology'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["FORENSIC"]),case=False) == True, 'spe_forensic'] = 1
data.spe_forensic = pd.to_numeric(data.spe_forensic).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_forensic'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["PLASTIC"]),case=False) == True, 'spe_plastic_surgeon'] = 1
data.spe_plastic_surgeon = pd.to_numeric(data.spe_plastic_surgeon).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_plastic_surgeon'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["PSYCHIATRISTS","PSYCHIATRY","PSYCHOLOGISTS"]),case=False) == True, 'spe_psycho'] = 1
data.spe_psycho = pd.to_numeric(data.spe_psycho).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_psycho'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["CARDIOLOGISTS","CARDIOLOGY","CARDIOTHORACIC"]),case=False) == True, 'spe_cardio'] = 1
data.spe_cardio = pd.to_numeric(data.spe_cardio).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_cardio'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["CHEMICAL"]),case=False) == True, 'spe_chemical'] = 1
data.spe_chemical = pd.to_numeric(data.spe_chemical).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_chemical'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["GASTROENTEROLOGISTS","GASTROENTEROLOGY"]),case=False) == True, 'spe_gastro'] = 1
data.spe_gastro = pd.to_numeric(data.spe_gastro).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_gastro'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["DENTISTS"]),case=False) == True, 'spe_dentist'] = 1
data.spe_dentist = pd.to_numeric(data.spe_dentist).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_dentist'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["SURGEONS"]),case=False) == True, 'spe_surgeon'] = 1
data.spe_surgeon = pd.to_numeric(data.spe_surgeon).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_surgeon'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["NEUROLOGY","NEUROSURGEONS","NEUROSURGERY"]),case=False) == True, 'spe_neurology'] = 1
data.spe_neurology = pd.to_numeric(data.spe_neurology).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_neurology'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["OBSTETRICS AND GYNAECOLOGY","OBSTETRICIANS-GYNAECOLOGISTS"]),case=False) == True, 'spe_gynacology'] = 1
data.spe_gynacology = pd.to_numeric(data.spe_gynacology).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_gynacology'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["ORAL"]),case=False) == True, 'spe_oral'] = 1
data.spe_oral = pd.to_numeric(data.spe_oral).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_oral'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["ORTHODONTICS","ORTHODONTISTS"]),case=False) == True, 'spe_orthodontists'] = 1
data.spe_orthodontists = pd.to_numeric(data.spe_orthodontists).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_orthodontists'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["ORTHOPAEDIC"]),case=False) == True, 'spe_orthopaedic'] = 1
data.spe_orthopaedic = pd.to_numeric(data.spe_orthopaedic).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_orthopaedic'] = np.nan
data.loc[data.speciality_1.str.contains('|'.join(["OTORHINOLARYNGOLOGISTS","OTORHINOLARYNGOLOGY"]),case=False) == True, 'spe_otorhino'] = 1
data.spe_otorhino = pd.to_numeric(data.spe_otorhino).fillna(0)
data.loc[data.speciality_1.isnull(), 'spe_otorhino'] = np.nan
data.groupby('speciality_1').size().reset_index()[30:75]




list(data.education.unique())
data.loc[data.education.str.contains("BACHELOR OF SURGERY",case=False) == True, 'edu_surgery'] = 1
data.edu_surgery = pd.to_numeric(data.edu_surgery).fillna(0)
data.loc[data.education.isnull(), 'edu_surgery'] = np.nan
data.loc[data.education.str.contains("BACHELOR OF MEDICINE",case=False)== True, 'edu_medicine'] = 1
data.edu_medicine = pd.to_numeric(data.edu_medicine).fillna(0)
data.loc[data.education.isnull(), 'edu_medicine'] = np.nan
data.loc[data.education.str.contains("MBBS",case=False)== True, 'edu_MBBS'] = 1
data.edu_MBBS = pd.to_numeric(data.edu_MBBS).fillna(0)
data.loc[data.education.isnull(), 'edu_MBBS'] = np.nan
data.loc[data.education.str.contains("MD",case=False)== True, 'edu_MD'] = 1
data.edu_MD = pd.to_numeric(data.edu_MD).fillna(0)
data.loc[data.education.isnull(), 'edu_MD'] = np.nan
data.loc[data.education.str.contains("PUBLIC HEALTH",case=False)== True, 'edu_public_health'] = 1
data.edu_public_health = pd.to_numeric(data.edu_public_health).fillna(0)
data.loc[data.education.isnull(), 'edu_public_health'] = np.nan
data.loc[data.education.str.contains("ORTHOPEDIC", case=False)== True, 'edu_orthopedic'] = 1
data.edu_orthopedic = pd.to_numeric(data.edu_orthopedic).fillna(0)
data.loc[data.education.isnull(), 'edu_orthopedic'] = np.nan
data.loc[data.education.str.contains("MS",case=False)== True, 'edu_MS'] = 1
data.edu_MS = pd.to_numeric(data.edu_MS).fillna(0)
data.loc[data.education.isnull(), 'edu_MS'] = np.nan
data.loc[data.education.str.contains("SPINE SURGERY",case=False)== True, 'edu_spine'] = 1
data.edu_spine = pd.to_numeric(data.edu_spine).fillna(0)
data.loc[data.education.isnull(), 'edu_spine'] = np.nan
data.loc[data.education.str.contains("SPORTS",case=False)== True, 'edu_sports'] = 1
data.edu_sports = pd.to_numeric(data.edu_sports).fillna(0)
data.loc[data.education.isnull(), 'edu_sports'] = np.nan
data.loc[data.education.str.contains("RADIOLOGY",case=False)== True, 'edu_radiology'] = 1
data.edu_radiology = pd.to_numeric(data.edu_radiology).fillna(0)
data.loc[data.education.isnull(), 'edu_radiology'] = np.nan
data.loc[data.education.str.contains("MSC",case=False)== True, 'edu_MSC'] = 1
data.edu_MSC = pd.to_numeric(data.edu_MSC).fillna(0)
data.loc[data.education.isnull(), 'edu_MSC'] = np.nan
data.loc[data.education.str.contains("BSC",case=False)== True, 'edu_BSC'] = 1
data.edu_BSC = pd.to_numeric(data.edu_BSC).fillna(0)
data.loc[data.education.isnull(), 'edu_BSC'] = np.nan
data.loc[data.education.str.contains("FRCS",case=False)== True, 'edu_FRCS'] = 1
data.edu_FRCS = pd.to_numeric(data.edu_FRCS).fillna(0)
data.loc[data.education.isnull(), 'edu_FRCS'] = np.nan
data.loc[data.edu_FRCS == 1]





list(data.affiliated_hospital.unique())
data.loc[data.affiliated_hospital.str.contains("MEDICAL CENTRE",case=False)== True, 'medical_centres'] = 1
data.medical_centres = pd.to_numeric(data.medical_centres).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'medical_centres'] = np.nan
data.loc[data.affiliated_hospital.str.contains("SPECIALIST",case=False)== True, 'special_hospitals'] = 1
data.special_hospitals = pd.to_numeric(data.special_hospitals).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'special_hospitals'] = np.nan
data.loc[data.affiliated_hospital.str.contains("UNIVERSITY",case=False)== True, 'university_hospital'] = 1
data.university_hospital = pd.to_numeric(data.university_hospital).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'university_hospital'] = np.nan
data.loc[data.affiliated_hospital.str.contains("EYE",case=False)== True, 'eye_hospital'] = 1
data.eye_hospital = pd.to_numeric(data.eye_hospital).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'eye_hospital'] = np.nan
data.loc[data.affiliated_hospital.str.contains('|'.join(["MATERNITY",'GYNO','Materniti']),case=False)== True, 'meterny_hospital'] = 1
data.meterny_hospital = pd.to_numeric(data.meterny_hospital).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'meterny_hospital'] = np.nan
data.loc[data.affiliated_hospital.str.contains("CANCER",case=False)== True, 'cancer_hospital'] = 1
data.cancer_hospital = pd.to_numeric(data.cancer_hospital).fillna(0)
data.loc[data.affiliated_hospital.isnull(), 'cancer_hospital'] = np.nan
data.loc[data.cancer_hospital == 1]



member = data.loc[data.memberships.notnull()]
member.memberships.unique()



data.loc[data.spe_urology == 1].groupby('state')['speciality_1'].size()
data.loc[data.eye_hospital == 1].groupby('state').size().sort_values(ascending = False)
data.loc[data.edu_MBBS == 1].groupby('state').size().sort_values(ascending = False)
data.loc[data.spe_psycho == 1].groupby('state').size().sort_values(ascending = False)
data.loc[data.special_hospitals == 1].groupby('state').size().sort_values(ascending = False)




data.internal_data.replace(0, np.nan, inplace=True)
data.mma_data.replace(0, np.nan, inplace=True)
data.nsr_data.replace(0, np.nan, inplace=True)
data.doc_data.replace(0, np.nan, inplace=True)




data.edu_BSC.replace(0, np.nan, inplace=True)
data.edu_FRCS.replace(0, np.nan, inplace=True)
data.edu_MBBS.replace(0, np.nan, inplace=True)
data.edu_MD.replace(0, np.nan, inplace=True)
data.edu_medicine.replace(0, np.nan, inplace=True)
data.edu_MS.replace(0, np.nan, inplace=True)
data.edu_MSC.replace(0, np.nan, inplace=True)
data.edu_orthopedic.replace(0, np.nan, inplace=True)
data.edu_public_health.replace(0, np.nan, inplace=True)
data.edu_radiology.replace(0, np.nan, inplace=True)
data.edu_spine.replace(0, np.nan, inplace=True)
data.edu_sports.replace(0, np.nan, inplace=True)
data.edu_surgery.replace(0, np.nan, inplace=True)




data.spe_anaesthesi.replace(0, np.nan, inplace=True)
data.spe_cardio.replace(0, np.nan, inplace=True)
data.spe_chemical.replace(0, np.nan, inplace=True)
data.spe_darmatology.replace(0, np.nan, inplace=True)
data.spe_dentist.replace(0, np.nan, inplace=True)
data.spe_endocrinology.replace(0, np.nan, inplace=True)
data.spe_family.replace(0, np.nan, inplace=True)
data.spe_forensic.replace(0, np.nan, inplace=True)
data.spe_gastro.replace(0, np.nan, inplace=True)
data.spe_gynacology.replace(0, np.nan, inplace=True)
data.spe_internal.replace(0, np.nan, inplace=True)
data.spe_neurology.replace(0, np.nan, inplace=True)
data.spe_oral.replace(0, np.nan, inplace=True)
data.spe_orthodontists.replace(0, np.nan, inplace=True)
data.spe_orthopaedic.replace(0, np.nan, inplace=True)
data.spe_otorhino.replace(0, np.nan, inplace=True)
data.spe_paediatrics.replace(0, np.nan, inplace=True)
data.spe_plastic_surgeon.replace(0, np.nan, inplace=True)
data.spe_psycho.replace(0, np.nan, inplace=True)
data.spe_sports.replace(0, np.nan, inplace=True)
data.spe_surgeon.replace(0, np.nan, inplace=True)
data.spe_urology.replace(0, np.nan, inplace=True)




data.medical_centres.replace(0, np.nan, inplace=True)
data.special_hospitals.replace(0, np.nan, inplace=True)
data.university_hospital.replace(0, np.nan, inplace=True)
data.eye_hospital.replace(0, np.nan, inplace=True)
data.meterny_hospital.replace(0, np.nan, inplace=True)
data.cancer_hospital.replace(0, np.nan, inplace=True)




data.lang_bahasa.replace(0, np.nan, inplace=True)
data.lang_chinese.replace(0, np.nan, inplace=True)
data.lang_english.replace(0, np.nan, inplace=True)
data.lang_hindi.replace(0, np.nan, inplace=True)
data.lang_malay.replace(0, np.nan, inplace=True)
data.lang_tamil.replace(0, np.nan, inplace=True)



################Positive Negative Univers####################
names = pd.read_csv('/home/ubuntu/data_analysis/mma/negative_mma/negative_names.csv')
neg_names = list(names.duplicated_name.unique())
internal = data.loc[data.internal_data == 1]
pos_names = list(internal.duplicated_name.unique())
dupl_neg_names = data.loc[data.duplicated_name.isin(neg_names)]
dupl_post_names = data.loc[data.duplicated_name.isin(pos_names)]
data['pos_neg_universe'] = ""
neg_index = dupl_neg_names.duplicated_name.index
pos_index = dupl_post_names.duplicated_name.index
data.pos_neg_universe.ix[pos_index] = 1
data.pos_neg_universe.ix[neg_index] = -1
data.pos_neg_universe.replace('',0, inplace=True)
data.pos_neg_universe.value_counts()




