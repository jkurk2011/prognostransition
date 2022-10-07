#!/usr/bin/env python
# coding: utf-8

# ## IQVIA Claim Line Import/Modify Data

# In[151]:
def isint(value):
  try:
    int(value)
    return str(int(value))
  except ValueError:
    return '0'


# Columns (5,17) have mixed types. Specify dtype option on import
rawmeddta['ndc_id'] = rawmeddta['ndc_cd'].fillna('0')
try:
	rawmeddta['ndc_id'] = rawmeddta['ndc_id'].astype(int).astype(str)
except: 
	rawmeddta['ndc_id'] = rawmeddta['ndc_id'].apply(lambda x: isint(x))
    
rawmeddta['ndc_id'] = rawmeddta['ndc_id'].apply(lambda x: None if x=='0' else x.zfill(11))

rawmeddta['claimid'] = rawmeddta['claimid'].astype('str')
rawmeddta['med_sample'] = np.where(rawmeddta.revenue_cd.isnull(), 'o', 'i')
rawmeddta = rawmeddta.dropna(subset=['fromdate']).rename(columns={
                                                                 "patient_id" : "mbr_pers_gen_key"})
rawmeddta['cpt_cd'] = rawmeddta['proccode']


# Columns (0) have mixed types. Specify dtype option on import or set low_memory=False.


rawrxdta['ndc_id'] = rawrxdta['ndc_code'].fillna(0).astype(int).astype(str)
rawrxdta['ndc_id'] = rawrxdta['ndc_id'].apply(lambda x: None if x=='0'  else x.zfill(11))
rawrxdta = rawrxdta.dropna(subset=['rx_month']).rename(columns={
                                                               "patient_id" : "mbr_pers_gen_key"})
rawrxdta['rx_month'] = pd.to_datetime(rawrxdta['rx_month'],format='%Y%m')


# ## IQVIA Submitted Census Info

# In[152]:


iqsubmit = iqsubmittail.copy()
iqsubmit['group_id'] =  iqsubmithead['group_id'].loc[0]
iqsubmit['effective_date'] = iqsubmithead['effective_date'].loc[0]
iqsubmit['underwriting_date'] = pd.to_datetime(date.today())
iqsubmit['subscriber_flag'] = iqsubmit['recordtype'].apply(lambda x: 1 if x==2 else 0).astype(bool)


# In[153]:




iqsubmit['age'] = (np.floor((pd.to_datetime(iqsubmit['effective_date']) - 
             pd.to_datetime(iqsubmit['pers_birth_date'])).dt.days / 365.25)).astype(int)

#iqsubmit=iqsubmit[iqsubmit['age']>=0] this was necessary to remove members not born as of uw date in POC sample


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


mbrrespdta = rawrxdta[['mbr_pers_gen_key','group_id']].\
    append(rawmeddta[['mbr_pers_gen_key', 'group_id']]).\
    drop_duplicates().merge(cust,on='group_id').drop_duplicates()

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

#add data_thru to cust df

#med
try:
    cust['data_thru_med']=rawmeddta.loc[0]['data_thru']
except:
    cust['data_thru_med'] = 200001
#rx

try:
    cust['data_thru_rx']=rawrxdta.loc[0]['data_thru']
except:
    cust['data_thru_rx'] = 200001
    

#merge data thru dates back into cust

#coerce to datetime with date as last day of month
if len(cust['data_thru_rx'].astype(str)[0]) == 8:
    cust.data_thru_rx = pd.to_datetime(cust['data_thru_rx'].astype(str))
else:
    cust.data_thru_rx = (pd.to_datetime(cust['data_thru_rx'].astype(str)+'01')
                         + pd.DateOffset(months = 1)
                         - pd.DateOffset(days = 1))
    
if len(cust['data_thru_med'].astype(str)[0]) == 8:
    cust.data_thru_med = pd.to_datetime(cust['data_thru_med'].astype(str))
else:
    cust.data_thru_med = (pd.to_datetime(cust['data_thru_med'].astype(str)+'01')
                         + pd.DateOffset(months = 1)
                         - pd.DateOffset(days = 1))

#return only unique rows
cust = cust.drop_duplicates()


