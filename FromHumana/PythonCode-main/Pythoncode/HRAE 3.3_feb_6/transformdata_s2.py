
mths = range(1,27)
cust['underwriting_date'] = pd.to_datetime(cust['underwriting_date'])
cust['mth_from_uwdate'] = 1
idx = pd.MultiIndex.from_product([mths, cust.group_id.unique()], names=['mth_from_uwdate', 'group_id'])
iqgrp_mthkey = cust.set_index(['mth_from_uwdate', 'group_id']).reindex(idx).fillna(0).reset_index()[['mth_from_uwdate', 'group_id']].merge(cust.drop(columns=['mth_from_uwdate']),on='group_id',how='inner')[['mth_from_uwdate', 'group_id', 'src_rpt_cust_id','underwriting_date','data_thru_med', 'data_thru_rx']]
#I think the below variables could be ceated more efficiently

iqgrp_mthkey['join_min_med'] = iqgrp_mthkey['data_thru_med'] - (iqgrp_mthkey.data_thru_med.dt.day -1)*pd.DateOffset(days=1) - (iqgrp_mthkey.mth_from_uwdate-1)*pd.DateOffset(months = 1)
iqgrp_mthkey['join_max_med'] = iqgrp_mthkey.join_min_med+pd.DateOffset(months = 1) - pd.DateOffset(days = 1)

iqgrp_mthkey['join_min_rx'] = iqgrp_mthkey['data_thru_rx'] - (iqgrp_mthkey.data_thru_rx.dt.day -1)*pd.DateOffset(days=1) - (iqgrp_mthkey.mth_from_uwdate-1)*pd.DateOffset(months = 1)
iqgrp_mthkey['join_max_rx'] = iqgrp_mthkey.join_min_rx+pd.DateOffset(months = 1) - pd.DateOffset(days = 1)


    
    
#create times
iqgrp_mthkey['times'] = iqgrp_mthkey['mth_from_uwdate'].apply(lambda x: 3 if x >=first_interval+7
                                                             else 2 if x> first_interval
                                                            else 1)


cust = cust.drop('mth_from_uwdate',axis=1)



#create qtr_meddta
rawmeddta.apply(lambda x: x.fillna(0,inplace=True) if x.name in ["procunits","charged"] else x)
rawmeddta["chargedsign"] = rawmeddta['charged'].apply(lambda a: (a>0) - (a<0))

qtr_meddta = pd.merge(rawmeddta, iqgrp_mthkey, how='inner', on='group_id')
qtr_meddta = qtr_meddta[(qtr_meddta['fromdate']>=qtr_meddta['join_min_med']) 
                        & (qtr_meddta['fromdate']<=qtr_meddta['join_max_med'])]


# ### Prep Med Diag Data



diags = [col for col in qtr_meddta if col.startswith('diag')]
diagvert = qtr_meddta.melt(id_vars = ["group_id",
                                      'src_rpt_cust_id',
                                      "mbr_pers_gen_key",
    "underwriting_date",
    "mth_from_uwdate",
    "times",                                 
    "med_sample"
                                     ],
    value_vars = diags,
    value_name = 'value').dropna(subset=['value']).drop(columns= 'variable')

ipclmids = qtr_meddta[qtr_meddta.med_sample =='i'].claimid
opclmids = qtr_meddta[qtr_meddta.med_sample =='o'].\
merge(diag_cptuse,  how='inner', left_on = 'proccode', right_on = 'cpt').claimid
clmiduse = pd.DataFrame(opclmids.append(ipclmids).drop_duplicates(), columns = ['claimid'])
qtr_dxdta = qtr_meddta.merge(clmiduse, how='inner')

debeligdxvert = qtr_dxdta.melt(id_vars = ["group_id",
                                      'src_rpt_cust_id',
                                      "mbr_pers_gen_key",
    "underwriting_date",
    "mth_from_uwdate",
    "times",                                 
    "med_sample"
                                     ],
    value_vars = diags,
    value_name = 'value').dropna(subset=['value']).drop(columns= 'variable')


iq_diagvert = pyqldf(dq)
iq_diagvert['value']=iq_diagvert['value'].astype('object')
iq_diagvert = iq_diagvert[iq_diagvert['value'].isin(icd10genetic['icd10'])==0]
# iq_diagvert.groupby('datatype').count()
# iq_diagvert[iq_diagvert['datatype']=='na']
# (icd09agnostic['diag_cd']=='7810').sum()
#missing leading 0's in icd9 table... should I force to character?
#Z0141 is non-billable, did we exclude these... is that why they don't join to HUCP table?


# ### Prep Med CPT Data

# In[157]:


# qtr_meddeb.info()
qtr_meddta_mc = qtr_meddta[(qtr_meddta['cpt_cd'].isnull()==0)]
qtr_meddta_mr = qtr_meddta[(qtr_meddta['cpt_cd'].isnull()==1) & (qtr_meddta['revenue_cd'].isnull()==0)]
qtr_meddta_mr2 = qtr_meddta[(qtr_meddta['cpt_cd'].isnull()==0) & (qtr_meddta['revenue_cd'].isnull()==0)]
iq_cptvert = pyqldf(qmc)

qtr_meddeb = iq_cptvert[iq_cptvert['debits'].isnull()==0]
if qtr_meddeb.shape[0]> 0:
    qtr_meddeb['ndc_allowed'] = qtr_meddeb[['ndc_allowed','cpt_allowed']].max(axis=1)
    debjoin1 = qtr_meddeb.groupby(['mbr_pers_gen_key',
                                  'src_rpt_cust_id',
                                  'group_id',
                                  'underwriting_date',
                                  'times',
                                  'med_sample',
                                  'milliman_match_name']).agg({'debits': max,
                                                              'ndc_allowed' : sum,
                                                              'cpt_allowed' : sum,
                                                              'cpt_cd' : len}).reset_index()
    debjoin2 = debjoin1.groupby(['mbr_pers_gen_key',
                                  'src_rpt_cust_id',
                                  'group_id',
                                  'underwriting_date',
                                  'med_sample',
                                  'times']).agg({'debits': sum,
                                                 'ndc_allowed' : sum,
                                                'cpt_allowed' : sum,
                                                'cpt_cd':sum}).reset_index().rename(columns={"cpt_cd":"distinct_ndcs"})
    debjoin2['debits'] = debjoin2['debits']/debjoin2['distinct_ndcs']
    debjoin2['debcost']=0
    debjoin2['debcost'][(debjoin2['times']==1)]=129*debjoin2['debits']/4
    debjoin2['debcost'][(debjoin2['times']==2)]=129*debjoin2['debits']/2
    debjoin2['debcost'][(debjoin2['times']==3)]=129*debjoin2['debits']
#     debjoin2 = debjoin2[(debjoin2['debcost'] > (debjoin2['ndc_allowed'].add(debjoin2['cpt_allowed'])) )]
    if meddcoverride ==1:
        debjoin2['debcost'] = meddcoverrideamt
    debjoin2 = debjoin2.drop(['ndc_allowed','cpt_allowed','distinct_ndcs','debits'],axis=1)
    debjoin3 = qtr_meddeb.merge(debjoin2)[['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times', 'ndc_id','cpt_cd', 'debcost','med_sample']].\
    groupby(['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times', 'ndc_id','cpt_cd', 'med_sample']).agg({'debcost':sum}).reset_index()
else:
    qtr_meddeb['debcost']=0
    debjoin3 = qtr_meddeb[['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times', 'ndc_id','cpt_cd', 'debcost','med_sample']]
    
iq_cpttimes = pyqldf(q2mc)
iq_revvert = pyqldf(qmr)
iq_revtimes = pyqldf(q2mr)
iq_revtimes['value'] = iq_revtimes['value'].astype('str').str.replace('.0','',regex=False)
iq_revtimes['value'] = iq_revtimes['value'].apply(lambda x: None if x=='nan' else x.zfill(4))
iq_revtimes2 = pyqldf(q2mr2)
iq_revtimes2['value'] = iq_revtimes2['value'].astype('str').str.replace('.0','',regex=False)
iq_revtimes2['value'] = iq_revtimes2['value'].apply(lambda x: None if x=='nan' else x.zfill(4))


medvertdta = pd.concat([iq_cpttimes, iq_revtimes, iq_revtimes2, iq_diagvert])
medvertdta['underwriting_date']=pd.to_datetime(medvertdta['underwriting_date'])


# ### Prep Rx Data

qtr_rxdta = (pd.merge(rawrxdta, iqgrp_mthkey, how='inner', on='group_id')
             .merge(ndcagnostic,how='left',on='ndc_id'))

qtr_rxdta = (qtr_rxdta[(qtr_rxdta['rx_month']>=qtr_rxdta['join_min_rx'])
                       & (qtr_rxdta['rx_month']<=qtr_rxdta['join_max_rx'])])

qtr_rxdta['ag_raw'] = qtr_rxdta[["days_supply_cnt", "serv_unit_cnt_max"]].min(axis=1)*qtr_rxdta['allowed_per_unit']
qtr_rxdta['ag_allow'] = np.where(qtr_rxdta['allowed'].isna(),qtr_rxdta['ag_raw'],qtr_rxdta['allowed'])
qtr_rxdta['ag_allow'] = np.where(qtr_rxdta['ag_allow'].isna(),0,qtr_rxdta['ag_allow'])
qtr_rxdta['allowed'] = qtr_rxdta['allowed'].fillna(0)
qtr_rxdeb = pd.merge(qtr_rxdta, hrdl_ndc_debits, how='inner', on='ndc_id')
if qtr_rxdeb.shape[0]> 0:
    rxdebjoin1 = qtr_rxdeb.groupby(['mbr_pers_gen_key',
                                  'src_rpt_cust_id',
                                  'group_id',
                                  'underwriting_date',
                                  'times',
                                  'milliman_match_name']).agg({'debits': max,
                                                              'ag_allow' : sum,
                                                               'ndc_id': len
                                                              }).reset_index()
    rxdebjoin2 = rxdebjoin1.groupby(['mbr_pers_gen_key',
                                  'src_rpt_cust_id',
                                  'group_id',
                                  'underwriting_date',
                                  'times'
                                    ]).agg({'debits': sum,
                                                 'ag_allow':sum,
                                           'ndc_id':sum }).reset_index().rename(columns={"ndc_id":"distinct_ndcs"})
    rxdebjoin2['debits'] = rxdebjoin2['debits']/rxdebjoin2['distinct_ndcs']
    rxdebjoin2['debcost']=0
    rxdebjoin2['debcost'][(rxdebjoin2['times']==1)]=129*rxdebjoin2['debits']/4
    rxdebjoin2['debcost'][(rxdebjoin2['times']==2)]=129*rxdebjoin2['debits']/2
    rxdebjoin2['debcost'][(rxdebjoin2['times']==3)]=129*rxdebjoin2['debits']
    rxdebjoin2 = rxdebjoin2[rxdebjoin2['debcost'] > rxdebjoin2['ag_allow']]
    rxdebjoin2 = rxdebjoin2.drop(['ag_allow', 'debits','distinct_ndcs'],axis=1)
    if rxdcoverride ==1:
        rxdebjoin2['debcost'] = rxdcoverrideamt
    rxdebjoin3 = qtr_rxdeb.merge(rxdebjoin2)[['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times', 'ndc_id', 'debcost']].groupby(['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times','ndc_id']).agg({'debcost':sum}).reset_index()

else:
    qtr_rxdeb['debcost']=0
    rxdebjoin3 = qtr_rxdeb[['group_id', 'src_rpt_cust_id',  'mbr_pers_gen_key', 'underwriting_date', 'times', 'ndc_id', 'debcost']]

rxvertdta= pyqldf(r2)
rxvertdta['underwriting_date']=pd.to_datetime(rxvertdta['underwriting_date'])


    
qtr_dxdeb = pd.merge(debeligdxvert, hrdl_diag_debits, how='inner', left_on ='value', right_on = 'diag_cd')
qtr_dxdeb = qtr_dxdeb[(qtr_dxdeb.times<=qtr_dxdeb.earliest_period) & (qtr_dxdeb.times >=qtr_dxdeb.latest_period)]
dx_debits_join = qtr_dxdeb.groupby(['mbr_pers_gen_key',
                                  'src_rpt_cust_id',
                                  'group_id',
                                  'underwriting_date',
                                  'diag_grouping']).agg({'plan_yr_dollar_assumption': max}).reset_index()\
                                    .rename(columns={"plan_yr_dollar_assumption":"med_debits"})

    
    

# In[159]:



# ## Export Modeling Data


rxspec = qtr_rxdeb[['src_rpt_cust_id', 'underwriting_date', 'times','milliman_match_name',
                   'plan_yr_dollar_assumption', 'earliest_period','latest_period']]
medspec = qtr_meddeb[['src_rpt_cust_id', 'underwriting_date', 'times','milliman_match_name',
                    'plan_yr_dollar_assumption', 'earliest_period','latest_period']]


rx_debits_join = rxspec.append(medspec)
rx_debits_join = rx_debits_join[(rx_debits_join.times<=rx_debits_join.earliest_period) & 
                                (rx_debits_join.times >=rx_debits_join.latest_period)
                  ]\
    .groupby(['src_rpt_cust_id', 
             'underwriting_date', 
             'milliman_match_name'
             ]
             , as_index=False)\
    .agg({'plan_yr_dollar_assumption':max})
    


debits_join = \
    rx_debits_join.rename(columns = {'plan_yr_dollar_assumption':'rx_debits'})\
    .groupby(['src_rpt_cust_id', 
              'underwriting_date'
             ]
             , as_index = False
            )\
    .agg({'rx_debits':sum}
     )\
    .append(dx_debits_join.rename(columns = {'plan_yr_dollar_assumption':'med_debits'})\
           .groupby(['src_rpt_cust_id', 
              'underwriting_date'
             ]
             , as_index = False
            )\
    .agg({'med_debits':sum}
        )).fillna(0)\
    .groupby(['src_rpt_cust_id', 
              'underwriting_date'
             ], as_index = False
            ).agg({'med_debits':sum,
                   'rx_debits':sum}
        ).fillna(0)
              

#filter to only include times == 1


#join to cust data
cust = cust.merge(debits_join[['underwriting_date', 'src_rpt_cust_id',
                              'rx_debits', 'med_debits']], how = 'left',
                  on = ['underwriting_date', 'src_rpt_cust_id'])

# In[165]:


# medvertdta.to_csv('Data/iq_med_vert.csv')
# rxvertdta.to_csv('Data/iq_rx_vert.csv')

# cust.to_csv('Data/iq_cust_lvl_info.csv')
# mbrrespdta.to_csv('Data/iq_mbr_lvl_info.csv')

#below two datasets do not need to be exported
# iq_rxspc.to_csv('Data/iq_rxspc.csv')
# iq_medspc.to_csv('Data/iq_medspc.csv')

