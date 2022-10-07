#!/usr/bin/env python
# coding: utf-8

# # Load the fitted models and features for scoring 

# In[ ]:
pfmsimple = pd.read_csv(mappath + 'hrdl_pfmsimple.csv')
ratecal = joblib.load(modelpath +'ratecal.json')
# try: 
gbm_Q1_xox = joblib.load(modelpath + 'gbm_Q1_xox.json')
gbm_Q2_xox = joblib.load(modelpath +'gbm_Q2_xox.json')
gbm_Q3_xox = joblib.load(modelpath +'gbm_Q3_xox.json')

gbm_Q1_ixx = joblib.load(modelpath +'gbm_Q1_ixx.json')
gbm_Q2_ixx = joblib.load(modelpath +'gbm_Q2_ixx.json')
gbm_Q3_ixx = joblib.load(modelpath +'gbm_Q3_ixx.json')

gbm_Q1_xxr = joblib.load(modelpath +'gbm_Q1_xxr.json')
gbm_Q2_xxr = joblib.load(modelpath +'gbm_Q2_xxr.json')
gbm_Q3_xxr = joblib.load(modelpath +'gbm_Q3_xxr.json')

# Load the fitted models and features for scoring 

xgb_ior = xgb.XGBRegressor()
xgb_ior.load_model(modelpath + 'ensemble_results.json')

xgb_op = xgb.XGBRegressor()
xgb_op.load_model(modelpath + 'ensemble_op.json')

xgb_op_rx = xgb.XGBRegressor()
xgb_op_rx.load_model(modelpath + 'ensemble_op_rx.json')

xgb_rx = xgb.XGBRegressor()
xgb_rx.load_model(modelpath + 'ensemble_rx.json')    

xgb_med = xgb.XGBRegressor()
xgb_med.load_model(modelpath + 'ensemble_med.json')

tfidfconverter_cpt_ip = joblib.load(modelpath+'tfidf_cpt_ip.json')
tfidfconverter_cpt_op = joblib.load(modelpath+'tfidf_cpt_op.json')

#load features in each of the medical/rx type data 
xox_sample =pd.DataFrame(columns= np.append(['src_rpt_cust_id'],pd.read_json(modelpath + 'xox_sample.json').columns))
ixx_sample = pd.DataFrame(columns= np.append(['src_rpt_cust_id'],pd.read_json(modelpath + 'ixx_sample.json').columns))
xxr_sample = pd.DataFrame(columns= np.append(['src_rpt_cust_id'],pd.read_json(modelpath + 'xxr_sample.json').columns))
grp_sampledta = pd.read_json(modelpath + 'grp_sampledta.json')
calibrated_sampledta = pd.read_json(modelpath + 'calibrated_sampledta.json')

xgb_grp = xgb.XGBRegressor()
xgb_grp.load_model(modelpath + 'xgb_grp.json')

mod_wls = joblib.load(modelpath + 'outputmod_wls.json')

# # Load Code Categorization Lookup Tables

# In[ ]:
####################LOAD FEATURE CATEGORIZATION TABLES#######################################

rxlookup2 = pd.read_csv(mappath + 'rx_lookup.csv')
rxlookup2['ndc_id'] =  rxlookup2['ndc_id'].apply(lambda x: str(x).zfill(11))

revlookup = pd.read_csv(mappath+'rev_manually_reduce.csv')
revlookup.columns = [x.lower() for x in revlookup]
revlookup = revlookup.drop(columns=['datatype'], axis=1)


cptlookup = pd.read_csv(mappath+'cpt_labels_v4.csv')
cptlookup.columns = [x.lower() for x in cptlookup]    

icd10lookup = pd.read_csv(mappath + 'hrdl_icd10_hcup_map.csv')
icd10lookup.columns = [x.lower() for x in icd10lookup]
icd10lookup.rename(columns={'prefx_map_id':'hcup'},inplace=True)

icd09lookup = pd.read_csv(mappath + 'hrdl_icd09_hcup_map.csv')
icd09lookup.columns = [x.lower() for x in icd09lookup]
icd09lookup.rename(columns={'prefx_map_id':'hcup'},inplace=True)

cptgenetic = pd.read_csv(mappath + 'hrdl_cpt_genetic_codes.csv')
cptgenetic.columns = [x.lower() for x in cptgenetic]

icd10genetic = pd.read_csv(mappath + 'hrdl_icd_genetic_codes.csv')
icd10genetic.columns = [x.lower() for x in icd10genetic]
# # Load Agnostic Unit Cost Tables

# In[ ]:

####################LOAD AGNOSTIC CLAIMS TABLES #######################################
medndcagnostic = pd.read_csv(mappath + 'hrdl_medndcagnostic.csv')
medndcagnostic.columns = [x.lower() for x in medndcagnostic]
medndcagnostic['ndc_id'] = medndcagnostic['ndc_id'].astype('str').str.zfill(11)
medndcagnostic=medndcagnostic[['ndc_id','serv_unit_cnt_max','allowed_per_unit']]

revagnostic = pd.read_csv(mappath + 'hrdl_revagnostic.csv')
revagnostic.columns = [x.lower() for x in revagnostic]
revagnostic=revagnostic[['revenue_cd','serv_unit_cnt_max','allowed_per_unit']]

ndcagnostic = pd.read_csv(mappath + 'hrdl_ndcagnostic.csv')
ndcagnostic.columns = [x.lower() for x in ndcagnostic]
ndcagnostic['ndc_id'] = ndcagnostic['ndc_id'].astype('str').str.zfill(11)
ndcagnostic=ndcagnostic[['ndc_id','serv_unit_cnt_max','allowed_per_unit']]

cptagnostic = pd.read_csv(mappath + 'hrdl_cptagnostic.csv')
cptagnostic.columns = [x.lower() for x in cptagnostic]
cptagnostic['cpt_cd'] = cptagnostic['hcpcs_cpt4_base_cd1']
cptagnostic=cptagnostic[['cpt_cd','serv_unit_cnt_max','allowed_per_unit']]


# In[ ]:


# In[ ]:
####################PREP SPECIALTY RX LOOKUP TABLES#######################################

hrdl_diag_debits = pd.read_csv(mappath + 'diag_debits.csv')
hrdl_diag_debits.columns = [x.lower() for x in hrdl_diag_debits]

diag_cptuse = pd.read_csv(mappath + 'cpt_dx_filtertable.csv')
diag_cptuse.columns = [x.lower() for x in diag_cptuse]

qdd = '''
select distinct
	diag_cd,
	earliest_period,
    latest_period,
    diag_grouping,
    max(diag_description) as diag_desc,
    min(plan_yr_dollar_assumption) as plan_yr_dollar_assumption
from hrdl_diag_debits 
where diag_cd is not null
group by 
    diag_cd,
    diag_grouping,
    earliest_period,
    latest_period
'''

hrdl_diag_debits = pyqldf(qdd)

qn = '''
select distinct
	ndc_id,
	earliest_period,
    latest_period,
	case 
		when max(milliman_match_name) = min(milliman_match_name) 
		then milliman_match_name else ndc_id 
	end as milliman_match_name,
    min(debits) as debits,
    min(plan_yr_dollar_assumption) as plan_yr_dollar_assumption
from hrdl_debits_clean 
where ndc_id is not null
group by 
    ndc_id,
    earliest_period,
    latest_period
'''



hrdl_debits_clean = pd.read_csv(mappath + 'final_debit_table.csv')
hrdl_debits_clean.columns = [x.lower() for x in hrdl_debits_clean]
hrdl_debits_clean['ndc_id'] = hrdl_debits_clean['ndc_id_sas'].str[1:]
hrdl_debits_clean = hrdl_debits_clean.drop(columns=['ndc_id_sas'], axis=1)


qn = '''
select distinct
	ndc_id,
	earliest_period,
    latest_period,
	case 
		when max(milliman_match_name) = min(milliman_match_name) 
		then milliman_match_name else ndc_id 
	end as milliman_match_name,
    min(debits) as debits,
    min(plan_yr_dollar_assumption) as plan_yr_dollar_assumption
from hrdl_debits_clean 
where ndc_id is not null
group by 
    ndc_id,
    earliest_period,
    latest_period
'''

hrdl_ndc_debits = pyqldf(qn)
#hrdl_ndc_debits['debits'].isna().sum() verified to match SAS null total

hrdl_hcpcs_debits_a = hrdl_debits_clean[hrdl_debits_clean['hcpcs_1'].isna()==0]\
.rename(columns={'hcpcs_1':'hcpcs'})\
.drop(columns=['hcpcs_2'],axis=1)
hrdl_hcpcs_debits_b = hrdl_debits_clean[hrdl_debits_clean['hcpcs_2'].isna()==0]\
.rename(columns={'hcpcs_2':'hcpcs'})\
.drop(columns=['hcpcs_1'],axis=1)
hrdl_debits_ab = hrdl_hcpcs_debits_a.append(hrdl_hcpcs_debits_b)

qh = '''
select distinct
	hcpcs,
    earliest_period,
    latest_period,
	case 
		when max(milliman_match_name) = min(milliman_match_name) 
		then milliman_match_name else hcpcs  
	end as milliman_match_name,
	min(coalesce(debits,0)) as debits,
	count(*) as appearances,
    min(coalesce(plan_yr_dollar_assumption,0)) as plan_yr_dollar_assumption
from hrdl_debits_ab
where hcpcs is not null
group by 
    hcpcs,
    earliest_period,
    latest_period
'''

hrdl_hcpcs_debits = pyqldf(qh)
