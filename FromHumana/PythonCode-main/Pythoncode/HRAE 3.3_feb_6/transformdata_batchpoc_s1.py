#!/usr/bin/env python
# coding: utf-8

# ## IQVIA Claim Line Import/Modify Data



def isint(value):
  try:
    int(value)
    return str(int(value))
  except ValueError:
    return '0'

# Columns (5,17) have mixed types. Specify dtype option on import
rawmeddta.columns = [x.lower() for x in rawmeddta]

rawmeddta['ndc_id'] = rawmeddta['ndc_cd'].fillna('0')
try:
	rawmeddta['ndc_id'] = rawmeddta['ndc_id'].astype(int).astype(str)
except: 
	rawmeddta['ndc_id'] = rawmeddta['ndc_id'].apply(lambda x: isint(x))
    
rawmeddta['ndc_id'] = rawmeddta['ndc_id'].apply(lambda x: None if x=='0' else x.zfill(11))


rawmeddta['claimid'] = rawmeddta['claimid'].astype('str')

try:
    rawmeddta['revenue_cd']
except:
    rawmeddta['revenue_cd'] = None    

rawmeddta['med_sample'] = np.where(rawmeddta.revenue_cd.isnull(), 'o', 'i')

rawmeddta = rawmeddta.dropna(subset=['fromdate']).rename(columns={
                                                                 "patient_id" : "mbr_pers_gen_key"})
rawmeddta['fromdate'] = pd.to_datetime(rawmeddta['fromdate'])
rawmeddta['cpt_cd'] = rawmeddta['proccode']


#filter rawmeddta based on logic from score iqvia program
if keep_i == False:
    rawmeddta = rawmeddta[rawmeddta['med_sample'] != 'i']
else:
    rawmeddta = rawmeddta
    
if keep_o == False:
    rawmeddta = rawmeddta[rawmeddta['med_sample'] != 'o']
else:
    rawmeddta = rawmeddta


# Columns (0) have mixed types. Specify dtype option on import or set low_memory=False.
rawrxdta.columns = [x.lower() for x in rawrxdta]
rawrxdta['ndc_id'] = rawrxdta['ndc_code'].fillna(0).astype(int).astype(str)
rawrxdta['ndc_id'] = rawrxdta['ndc_id'].apply(lambda x: None if x=='0'  else x.zfill(11))

rawrxdta = rawrxdta.dropna(subset=['rx_month']).rename(columns={
                                                               "patient_id" : "mbr_pers_gen_key"})
rawrxdta['rx_month'] = pd.to_datetime(rawrxdta['rx_month'],format='%Y%m')

rawrxdta['rx_sample'] = 'r'

#filter based on logic from socre iqvia program

if keep_r == False:
    rawrxdta = rawrxdta[rawrxdta['rx_sample'] != 'r']
else:
    rawrxdta = rawrxdta

# ## IQVIA Submitted Census Info

# In[5]:


#below: fit "rate_calibrated" ... this was fit in the SAS modeling dataset and I've verified I get same parameters
# mbrrespdta = pd.read_csv('/advanalytics/pgk0233/data/hrdl_model_planyr.csv')
# mbrrespdta=mbrrespdta[mbrrespdta['train_subpartition']=='train']
# seg_dummy = pd.get_dummies(mbrrespdta['bus_segment_cd'])
# seg_dummy.columns = ['bus_segment_' + x for x in seg_dummy]
# y_train = mbrrespdta[['mbr_mth_ag_cmpl_allow']]
# x_train = pd.concat([seg_dummy, mbrrespdta[['comm_rt','lf_group','aso_group']]],axis=1).drop('bus_segment_T',axis=1)
# ratecal = LinearRegression(normalize=False)
# ratecal.fit(x_train, y_train)  print(ratecal.intercept_,ratecal.coef_)
# joblib.dump(ratecal, 'IQVIA_Score/modelobjects/'+'ratecal.json')

# iqsubmit = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/Data/20190904_IQVIA_poc60ksampleadhoc.csv'
#                       )
#In production "underwriting_date" is "today" pers_end_date is first day of that month, 
# effective date is whatever is on the quotet
iqsubmit['underwriting_date'] = pd.to_datetime(iqsubmit['pers_end_date'], format = '%Y%m')+pd.DateOffset(months = 1)
iqsubmit['effective_date'] = pd.to_datetime(iqsubmit['pers_end_date'], format = '%Y%m')+pd.DateOffset(months = 4)
iqsubmit['pers_birth_date'] = pd.to_datetime(iqsubmit['pers_birth_date'], format = '%Y%m%d')

iqsubmit['age'] = (np.floor((pd.to_datetime(iqsubmit['effective_date']) - 
             pd.to_datetime(iqsubmit['pers_birth_date'])).dt.days / 365.25)).astype(int)

#iqsubmit=iqsubmit[iqsubmit['age']>=0] this was necessary to remove members not born as of uw date in POC sample

#Manual rate agnostic table

#grabbing actual values for segment, cr_ind, and lf_group.  In practice, we'll assume something
#also grabbing the 


#PFM rel generation

iqsubmit = pyqldf(pfm)

#Final Export Code
cust = iqsubmit.groupby(['group_id', 'underwriting_date']).agg(
{'pers_first_name' : len,
 'age_gender_factor' : lambda x: x.mean()
}).reset_index().rename(columns={"pers_first_name": "quoted_members",
                                "age_gender_factor" : "quoted_pfm_rel"})
cust['train_subpartition'] = 'valid_holdout'



# cust = cust.merge(iqgrp_resp.drop('underwriting_date',axis=1), how = 'inner',on = 'group_id')
cust['lf_group'] = cust['quoted_members'].apply(lambda x: 1 if x <50 else 0)
cust['aso_group'] = cust['quoted_members'].apply(lambda x: 1 if x <50 else 0)
cust['bus_segment_cd'] = 'T'
cust['comm_rt'] = 0
cust['src_rpt_cust_id'] = cust['group_id'] #name "src_rpt_cust_id" required for scoring code


mbrrespdta = rawrxdta[['mbr_pers_gen_key','group_id']].    append(rawmeddta[['mbr_pers_gen_key', 'group_id']]).    drop_duplicates().merge(cust,on='group_id').drop_duplicates()

mbrrespdta['hist_mths_avail'] = 24
x_valid= pd.DataFrame(columns=['bus_segment_J',
                               'bus_segment_L',
                               'bus_segment_N',
                               'bus_segment_P',
                               'bus_segment_S',
                               'comm_rt',
                               'lf_group',
                               'aso_group'])

x_valid[['bus_segment_J']] = (mbrrespdta[['bus_segment_cd']]=='J').astype(int)
x_valid[['bus_segment_L']] = (mbrrespdta[['bus_segment_cd']]=='L').astype(int)
x_valid[['bus_segment_N']] = (mbrrespdta[['bus_segment_cd']]=='N').astype(int)
x_valid[['bus_segment_P']] = (mbrrespdta[['bus_segment_cd']]=='P').astype(int)
x_valid[['bus_segment_S']] = (mbrrespdta[['bus_segment_cd']]=='S').astype(int)
x_valid[['comm_rt','lf_group','aso_group']] = mbrrespdta[['comm_rt','lf_group','aso_group']]
# x_valid
mbrrespdta['rate_calibrated'] = ratecal.predict(x_valid)
mbrrespdta['underwriting_date']=pd.to_datetime(mbrrespdta['underwriting_date'])

# add data_thru variable as as day of two months prior to the UW
cust['data_thru_med'] = pd.to_datetime(cust.underwriting_date) - pd.DateOffset(months = 1) - pd.DateOffset(days = 1)
cust['data_thru_rx'] = cust.data_thru_med

        



