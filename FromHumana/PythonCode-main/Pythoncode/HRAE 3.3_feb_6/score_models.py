#!/usr/bin/env python
# coding: utf-8

# ###  Structure 
# Score the member level claim by the fitted model 
# 
# Step 1: Pull medical data and rx data 
# 
# Step 2: Score mbr member level claim  
# 
# Step 3: score group level claim & output group predictions 
# 
# -----------------------------------------------------------------
# ### Input:
# 
# If member level claims have been generated:
# 
#     Dataframe predictions_all should be feeded before run the program 
#     
# If member level claims havn't been generated:
# 
#     1. Fit member level and group level model or load the models (run load_model.ipynb model)
#     2. Load medical/rx records 
# 
# Group level featurs from data frame cust_lvl_info, ie, group segements, size of the group et. 
# 
# ### Output: 
# 
# 1. member level prediction, including member id keys, ip/op/rx scores and final score, saved in dataframe named predictions_all 
# 2. group level prediction, including group id keys, group claims prediction (pred_grp), saved in dataframe named cust_pred 

# In[ ]:



cali_feature = ['quoted_members', 'comm_rt', 'bus_segment_T',  'quoted_pfm_rel' ]

lkbkkeycols = ['mbr_pers_gen_key','underwriting_date', 'src_rpt_cust_id'] 
lkbkkeyall = ['mbr_pers_gen_key','underwriting_date', 'src_rpt_cust_id','times'] 
keycols = ['mbr_pers_gen_key', 'underwriting_date', 'src_rpt_cust_id', 'mbr_mth_ag_cmpl_allow' ]
grplkbkkey = ['src_rpt_cust_id', 'underwriting_date']
keepcols = ['occurrences', 'units_per_occurrence', 'charge_amt', 'allow_amt', 'ag_allow_amt']
subset_cols = ['train_subpartition']
response = ['mbr_mth_ag_cmpl_allow']

cust['total_members'] = cust['quoted_members']
cust['quoted_members'] =  cust['quoted_members'].apply(lambda x: min(max(10,x),1000))

# In[ ]:



######## define repeated used functions 
#set up the ids columns 

perskey0 = mbrrespdta[lkbkkeycols + subset_cols + ['rate_calibrated', 'hist_mths_avail']] 

perskey0['one'] = 1
qs=pd.DataFrame({'times': range(1,4),
                   'one' : 1})
perskey0 = pd.merge(perskey0, qs, on='one', how='inner').drop('one',1)

def joincodes(spl, perskey = perskey0):   

    if(spl[0]== "i"):
        if revdevertdta.shape[0]>0:
            perskey = perskey.merge(revdevertdta, how='left', on= lkbkkeycols + [ 'times'] )
        if hcupdevertdta.shape[0]>0:            
            perskey = perskey.merge(hcupdevertdta, how='left', on= lkbkkeycols+ [ 'times'] )
        if cpttfidfdta.shape[0]>0:
            perskey = perskey.merge(cpttfidfdta, how='left', on=lkbkkeycols+ [ 'times'] )
        perskey = perskey.merge(ag_pmpm[lkbkkeycols+ [ 'ag_allow_ip_pmpm']], how='left', on=lkbkkeycols )  
        perskey['has_med'] = perskey['has_rev'] + perskey['has_hcup'] + perskey['has_cpt']
    elif(spl[1] == "o"):
        if hcupdevertdta.shape[0]>0:            
            perskey = perskey.merge(hcupdevertdta, how='left', on= lkbkkeycols+ [ 'times'] )
        if cpttfidfdta.shape[0]>0:
            perskey = perskey.merge(cpttfidfdta, how='left', on=lkbkkeycols+ [ 'times'] )
       
        perskey=   perskey.merge(ag_pmpm[lkbkkeycols+ [ 'ag_allow_op_pmpm']], how='left', on=lkbkkeycols )  
        perskey['has_med'] =  perskey['has_hcup'] + perskey['has_cpt']

    else:
        perskey = perskey
    if(spl[2]== "r"):
        if gpidevertdta.shape[0]>0:            
            perskey = perskey.merge(gpidevertdta, how='left', on= lkbkkeycols+ ['times'] )        .merge(ag_pmpm[lkbkkeycols+ [ 'ag_allow_rx_pmpm']], how='left', on=lkbkkeycols )        
    perskey = perskey.fillna(0)

    return perskey 


def part(da, partition):
    out=da[(da[subset_cols]== partition)]
#     return(np.asarray(out))
#     return(pd.DataFrame(out))
    return(out)
def part2(da):
    return(part(da,'train'), part(da,'valid_paramopt'),           part(da,'valid_holdout'))

def formatout(indta):
    return(np.asarray(indta.drop(subset_cols,1)))   
    
def bc_trans(x, lamb= 0):
    if lamb==0:        
        return np.log(x + 0.001)
    else:
        return (np.power(x+ 0.0001, lamb) - 1)/lamb  
    
def dateformat(df, col = 'underwriting_date'):    
    dt_str = list(df[col].unique())
    dt_time = pd.to_datetime(dt_str)
    dt_map = {}
    for i in range(len(dt_str)):
        dt_map[dt_str[i]] = dt_time[i] 
    for a in [col]:
        df[a]=df[a].map(lambda x: dt_map[x])
    return df 

# # Score member level claims 

# In[ ]:


ag_pmpm = mbrrespdta[lkbkkeycols]


# In[ ]:


#generate ip features and score them 

med_i = medvertdta[(medvertdta['med_sample'] == 'i' )]
if med_i.shape[0]>0:

    hcupvert = med_i[(med_i['datatype'] == 'md') ]
    cptvert =  med_i[(med_i['datatype'] == 'mc') ]
    revvert =  med_i[(med_i['datatype'] == 'mr') ]


##########################################################################################################
    if hcupvert.shape[0] > 0:
        #hcupvert['value'] = 'hcup_' + hcupvert['value'].astype('str')
        dummy = pd.get_dummies(hcupvert['value'])
        dummy = pd.concat([hcupvert[lkbkkeyall], dummy], axis = 1)
        #check the existance of 
        hcupdevertdta = dummy.groupby(lkbkkeyall).sum().astype(bool).reset_index()
        #use the mean of the occurence 
        #hcupdevertdta = dummy.groupby(lkbkkeyall).mean().reset_index()

        hcupcols = [col for col in hcupdevertdta if col.startswith('hcup')]
        hcupdevertdta['num_hcup'] = hcupdevertdta[hcupcols].sum(axis = 1)
        hcupdevertdta['has_hcup'] = hcupdevertdta[hcupcols].apply(lambda x : sum(x), axis = 1).astype(bool)

        del dummy,  hcupvert
    else:
        hcupdevertdta = medvertdta[lkbkkeycols + ['times']].iloc[:1]
        hcupdevertdta['has_hcup'] = 0
        hcupdevertdta['num_hcup'] = 0
##########################################################################################################
    if cptvert.shape[0]>0:

        cpt_ag_allow = cptvert.groupby(lkbkkeyall)['ag_allow_amt'].sum().reset_index()
        cpt_ag_allow.rename(columns = {'ag_allow_amt' : 'cpt_ag_allow_amt'}, inplace = True)
        #average pmpm by dediving to the hist_mths_avail, if don't have, assume 24 
        cptvert = cptvert.merge(cptlookup,how='left',on='value')
        cptvert['prefx_map_id'] = cptvert['prefx_map_id'].fillna('cpt_unk')
        cptdevert = pd.DataFrame(cptvert.groupby(lkbkkeyall)['prefx_map_id'].apply(list))        
        
        cptdevert = cptdevert.reset_index() 

        cptf = lambda x: ' '.join([item for item in x ])
        cptdevert["prefx_map_id"]=cptdevert["prefx_map_id"].apply(cptf)

        X_cpt = cptdevert.prefx_map_id.values
        

        cpttfidfdta = tfidfconverter_cpt_ip.transform(X_cpt).toarray().astype('float16')

        cpttfidfdta = pd.DataFrame(cpttfidfdta)
        cpttfidfdta.columns = tfidfconverter_cpt_ip.get_feature_names()
        cpttfidfdta = pd.concat([cptdevert[lkbkkeyall].reset_index(), cpttfidfdta ], axis = 1).drop(columns='index')
        cpttfidfdta = cpttfidfdta.groupby(lkbkkeyall).mean().reset_index()

        cptcols = [col for col in cpttfidfdta if col.startswith('cpt')]
        cpttfidfdta['sum_cpt'] = cpttfidfdta[cptcols].sum(axis = 1)
        cpttfidfdta['has_cpt'] = (cpttfidfdta['sum_cpt'] > 0 ) 

        cpttfidfdta = cpttfidfdta.merge(cpt_ag_allow[lkbkkeyall + ['cpt_ag_allow_amt']], on = lkbkkeyall, how = 'outer')
        del cptvert, cptdevert, X_cpt
    else:
        cpttfidfdta = medvertdta[lkbkkeycols+ ['times']].iloc[:1]
        cpttfidfdta['has_cpt'] = 0

##########################################################################################################
    if revvert.shape[0]>0:            
        rev_ag_allow = revvert.groupby(lkbkkeyall )['ag_allow_amt'].sum().reset_index()
        rev_ag_allow.rename(columns = {'ag_allow_amt' : 'rev_ag_allow_amt'}, inplace = True)
        revvert = revvert.merge(revlookup,how='left',on='value')                
        revvert['prefx_map_id'] =  revvert['prefx_map_id'].fillna('rev_unk')

        dummy = pd.get_dummies(revvert['prefx_map_id'])
        dummy = pd.concat([revvert[lkbkkeycols +['times'] ], dummy], axis = 1)
        revdevertdta = dummy.groupby(lkbkkeyall).sum().astype(bool).reset_index()
        #use avg times  
        #revdevertdta = revdevertdta.groupby(lkbkkeycols +['times']).mean().reset_index()
        revcols = [col for col in revdevertdta if col.startswith('rev')]
        revdevertdta['num_rev'] = revdevertdta[revcols].sum(axis = 1)
        revdevertdta['has_rev'] = 1
        revdevertdta = revdevertdta.merge(rev_ag_allow[lkbkkeyall + ['rev_ag_allow_amt']], on = lkbkkeyall, how = 'outer')
        
    else:
        revdevertdta = medvertdta[lkbkkeycols+['tims']].iloc[:1]
        revdevertdta['has_rev'] = 0

##########################################################################################################
##########################################################################################################
#create score 

#aggreage totol agnostic amount 
    ag_pmpm_i = med_i.pivot_table(index = lkbkkeycols, values = 'ag_allow_amt', columns = ['med_sample'], aggfunc = np.sum).fillna(0).reset_index()
#average pmpm by dediving to the hist_mths_avail, if don't have, assume 24 
    ag_pmpm_i['ag_allow_ip_pmpm'] = ag_pmpm_i['i']/24
    ag_pmpm = ag_pmpm.merge(ag_pmpm_i[lkbkkeycols + ['ag_allow_ip_pmpm']], on = lkbkkeycols , how = 'left')


    spl = 'ixx'
    perskey = joincodes(spl, perskey = perskey0)
    cols = list(ixx_sample.drop( subset_cols + response , axis =1).columns )
    perskey =  perskey.reindex(columns=cols)
    bcols = [x for x in perskey if x.startswith(('rev', 'hcup')) ]   
    perskey = perskey.fillna(0)
    perskey[bcols] = perskey[bcols].astype(bool)
    

    #perskey =  perskey.fillna(0)
    #perskey['has_med'] = perskey['has_hcup'] + perskey['has_cpt']  + perskey['has_rev']


    xdta = perskey[(perskey['times']==1) & (perskey['has_med'] > 0)].drop( lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==1)  & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)

    prediction = ydta
    if prediction.shape[0]> 0:
        prediction['pred_ixx_1'] = gbm_Q1_ixx.predict(xdta)
    else:
        prediction['pred_ixx_1'] = None

    xdta = perskey[(perskey['times']==2) & (perskey['has_med'] > 0)].drop(lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==2) & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)
    pred_temp = ydta
    if pred_temp.shape[0]> 0:
        pred_temp['pred_ixx_2'] = gbm_Q2_ixx.predict(xdta)
    else:
        pred_temp['pred_ixx_2'] = None
    prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')
    
    xdta = perskey[(perskey['times']==3) & (perskey['has_med'] > 0)].drop(lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==3) & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)
    pred_temp = ydta
    if pred_temp.shape[0]> 0:
        pred_temp['pred_ixx_3'] = gbm_Q3_ixx.predict(xdta)
    else:
        pred_temp['pred_ixx_3'] = None
        
    prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')
    pred_ip = prediction 
else: 
    pred_ip = perskey0[lkbkkeycols+ ['times']].iloc[:0]


# In[ ]:


#generate op features and score them 


med_o = medvertdta[(medvertdta['med_sample'] == 'o' ) ]
if med_o.shape[0]>0:

    hcupvert = med_o[(med_o['datatype'] == 'md') ]
    cptvert =  med_o[(med_o['datatype'] == 'mc') ]


##########################################################################################################
    if hcupvert.shape[0] > 0:
        #hcupvert['value'] = 'hcup_' + hcupvert['value'].astype('str')
        dummy = pd.get_dummies(hcupvert['value'])
        dummy = pd.concat([hcupvert[lkbkkeyall], dummy], axis = 1)
        #check the existance of 
        hcupdevertdta = dummy.groupby(lkbkkeyall).sum().astype(bool).reset_index()

        hcupcols = [col for col in hcupdevertdta if col.startswith('hcup')]
        hcupdevertdta['num_hcup'] = hcupdevertdta[hcupcols].sum(axis = 1)
        hcupdevertdta['has_hcup'] = (hcupdevertdta[hcupcols].sum(axis = 1) > 0)

    else:
        hcupdevertdta = medvertdta[lkbkkeycols+ ['times']].iloc[:1]
        hcupdevertdta['has_hcup'] = 0

##########################################################################################################
    if cptvert.shape[0]>0:

        cpt_ag_allow = cptvert.groupby(lkbkkeyall)['ag_allow_amt'].sum().reset_index()
        cpt_ag_allow.rename(columns = {'ag_allow_amt' : 'cpt_ag_allow_amt'}, inplace = True)
        #average pmpm by dediving to the hist_mths_avail, if don't have, assume 24 
        cptvert = cptvert.merge(cptlookup,how='left',on='value')
        cptvert['prefx_map_id'] = cptvert['prefx_map_id'].fillna('cpt_unk')
        cptdevert = pd.DataFrame(cptvert.groupby(lkbkkeycols + ['times'])['prefx_map_id'].apply(list))
        cptdevert = cptdevert.reset_index()         
        
        cptf = lambda x: ' '.join([item for item in x ])
        cptdevert["prefx_map_id"]=cptdevert["prefx_map_id"].apply(cptf)

        X_cpt = cptdevert.prefx_map_id.values
  

        cpttfidfdta = tfidfconverter_cpt_op.transform(X_cpt).toarray().astype('float16')

        cpttfidfdta = pd.DataFrame(cpttfidfdta)
        cpttfidfdta.columns = tfidfconverter_cpt_op.get_feature_names()
        cpttfidfdta = pd.concat([cptdevert[lkbkkeyall].reset_index(), cpttfidfdta ], axis = 1).drop(columns='index')
        cpttfidfdta = cpttfidfdta.groupby(lkbkkeyall).mean().reset_index()

        cptcols = [col for col in cpttfidfdta if col.startswith('cpt')]
        cpttfidfdta['sum_cpt'] = cpttfidfdta[cptcols].sum(axis = 1)
        cpttfidfdta['has_cpt'] = 1
        
        cpttfidfdta = cpttfidfdta.merge(cpt_ag_allow[lkbkkeyall + ['cpt_ag_allow_amt']], on = lkbkkeyall, how = 'outer')
        
        del cptvert, cptdevert, X_cpt 

    else:
        cpttfidfdta = medvertdta[lkbkkeycols + ['times']].iloc[:1]
        cpttfidfdta['has_cpt'] = 0


##########################################################################################################
##########################################################################################################
#create score 

    #aggreage totol agnostic amount 
    ag_pmpm_o = med_o.pivot_table(index = lkbkkeycols, values = 'ag_allow_amt', columns = ['med_sample'], aggfunc = np.sum).fillna(0).reset_index()
    #average pmpm by dediving to the hist_mths_avail, if don't have, assume 24 
 
    ag_pmpm_o['ag_allow_op_pmpm'] = ag_pmpm_o['o']/24
    ag_pmpm = ag_pmpm.merge(ag_pmpm_o[lkbkkeycols + ['ag_allow_op_pmpm']], on = lkbkkeycols , how = 'left')


    spl = 'xox'
    perskey = joincodes(spl, perskey = perskey0)
    cols = list(xox_sample.drop( subset_cols + response , axis =1).columns )
    perskey =  perskey.reindex(columns=cols)
    bcols = [x for x in perskey if x.startswith(('rev', 'hcup'))  ]           
    perskey = perskey.fillna(0)
    perskey[bcols] = perskey[bcols].astype(bool)

    #perskey =  perskey.fillna(0)
    #perskey['has_med'] = perskey['has_hcup'] + perskey['has_cpt']  + perskey['has_rev']


    xdta = perskey[(perskey['times']==1) & (perskey['has_med'] > 0)].drop( lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==1)  & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)
    prediction = ydta
    if prediction.shape[0]> 0:
        prediction['pred_xox_1'] = gbm_Q1_xox.predict(xdta)
    else:
        prediction['pred_xox_1'] = None
        
    xdta = perskey[(perskey['times']==2) & (perskey['has_med'] > 0)].drop(lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==2) & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)
    pred_temp = ydta
    if pred_temp.shape[0]> 0:
        pred_temp['pred_xox_2'] = gbm_Q2_xox.predict(xdta)
    else:
        pred_temp['pred_xox_2'] = None
    prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')

    xdta = perskey[(perskey['times']==3) & (perskey['has_med'] > 0)].drop(lkbkkeycols +   [ 'times'], 1)
    ydta = perskey[(perskey['times']==3) & (perskey['has_med'] > 0)][lkbkkeycols]
    xdta = np.array(xdta)
    pred_temp = ydta
    if pred_temp.shape[0]> 0:
        pred_temp['pred_xox_3'] = gbm_Q3_xox.predict(xdta)
    else:
        pred_temp['pred_xox_3'] = None
    prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')
    pred_op = prediction 
else:
    pred_op = perskey0[lkbkkeycols].iloc[0]


# In[ ]:


#generate rx features and score them 

##########################################################################################################
if rxvertdta.shape[0] > 0:
    rxvert = rxvertdta.copy()            
    rx_ag_allow = rxvertdta.groupby(lkbkkeycols+['times'] )['ag_allow_amt'].sum().reset_index()
    rx_ag_allow.rename(columns = {'ag_allow_amt' : 'rx_ag_allow_amt'}, inplace = True)

    rxvert = rxvert.merge(rxlookup2,how='left',on='ndc_id')
    rxvert['pref_map_id'] = rxvert['pref_map_id'].fillna('gpi_unk')    
    
    #rxvert =rxvert[lkbkkeycols + ['times', 'pref_map_id']].drop_duplicates()
    if rxvert.shape[0] > 0:
        dummy = pd.get_dummies(rxvert['pref_map_id'])
        dummy = pd.concat([rxvert[lkbkkeycols +['times'] ], dummy], axis = 1)
        gpidevertdta = dummy.groupby(lkbkkeyall).sum().astype(bool).reset_index()
        gpidevertdta = gpidevertdta.merge(rx_ag_allow, on = lkbkkeycols + ['times'] , how = 'outer')
        
        #gpidevertdta = gpidevertdta.groupby(['mbr_pers_gen_key', 'underwriting_date', 'times']).mean().reset_index()
        gpicols = [col for col in gpidevertdta if col.startswith('gpi')]
        #gpidevertdta['num_gpi'] = gpidevertdta[gpicols].sum(axis = 1).astype(bool)
        #
        gpidevertdta['num_gpi'] = gpidevertdta[gpicols].sum(axis = 1).astype(bool)
        gpidevertdta['has_gpi'] = 1

        del rxvert, dummy
        
    #create score 

        ag_pmpm_rx = rxvertdta.pivot_table(index = lkbkkeycols, values = 'ag_allow_amt', columns = ['rx_sample'], 
                                           aggfunc = np.sum).fillna(0).reset_index()
        ag_pmpm_rx = ag_pmpm_rx.merge(mbrrespdta[lkbkkeycols ], on =lkbkkeycols, how = 'inner')

        ag_pmpm_rx['ag_allow_rx_pmpm'] = ag_pmpm_rx['r']/24
        ag_pmpm = ag_pmpm.merge(ag_pmpm_rx[lkbkkeycols + ['ag_allow_rx_pmpm']], on = lkbkkeycols , how = 'left')

        spl = 'xxr'
        perskey = joincodes(spl, perskey = perskey0)
        cols = list(xxr_sample.drop(subset_cols + response, axis =1).columns )
        perskey =  perskey.reindex(columns=cols)
        perskey =  perskey.fillna(0)

        bcols = [x for x in perskey if x.startswith(('gpi')) ]            
        perskey[bcols] = perskey[bcols].astype(bool)

        xdta = perskey[(perskey['times']==1) & (perskey['has_gpi'] > 0)].drop(lkbkkeycols +  [ 'times'], 1)
        ydta = perskey[(perskey['times']==1)  & (perskey['has_gpi'] > 0)][lkbkkeycols ]
        xdta = np.array(xdta)

        prediction = ydta

        if prediction.shape[0]> 0:
            prediction['pred_xxr_1'] = gbm_Q1_xxr.predict(xdta)
        else:
            prediction['pred_xxr_1'] = None

        xdta = perskey[(perskey['times']==2) & (perskey['has_gpi'] > 0)].drop(lkbkkeycols +  [ 'times'], 1)
        ydta = perskey[(perskey['times']==2) & (perskey['has_gpi'] > 0)][lkbkkeycols]
        xdta = np.array(xdta)
        pred_temp = ydta
        if pred_temp.shape[0]> 0:
            pred_temp['pred_xxr_2'] = gbm_Q2_xxr.predict(xdta)
        else:
            pred_temp['pred_xxr_2'] = None
        prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')

        xdta = perskey[(perskey['times']==3) & (perskey['has_gpi'] > 0)].drop(lkbkkeycols +   [ 'times'], 1)
        ydta = perskey[(perskey['times']==3) & (perskey['has_gpi'] > 0)][lkbkkeycols]
        xdta = np.array(xdta)
        pred_temp = ydta
        if pred_temp.shape[0]> 0:
            pred_temp['pred_xxr_3'] = gbm_Q3_xxr.predict(xdta)
        else:
            pred_temp['pred_xxr_3'] = None
        prediction = prediction.merge(pred_temp, on = lkbkkeycols, how = 'outer')
        pred_rx = prediction    
else:
    pred_rx = perskey0[lkbkkeycols+ ['times']].iloc[:]
    gpidevertdta = medvertdta[lkbkkeycols].iloc[:1]
    gpidevertdta['has_gpi'] = 0
    gpidevertdta['num_gpi'] = 0
# In[ ]:

##########################################################################################################
##########################################################################################################

#ensemble the score of ip/op/rx and predict member level claims, output 'score' as the prediction of the claim 

predictions_all  =  mbrrespdta[lkbkkeycols].merge(ag_pmpm, on = lkbkkeycols, how = 'left')
if (med_i.shape[0] > 0):
    predictions_all = predictions_all.merge(pred_ip[lkbkkeycols + ['pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3', ]], on = lkbkkeycols, how = 'outer')
if (med_o.shape[0] > 0):
    predictions_all = predictions_all.merge(pred_op[lkbkkeycols + ['pred_xox_1',  'pred_xox_2', 'pred_xox_3']], on = lkbkkeycols, how = 'outer')
if rxvertdta.shape[0] > 0:
    predictions_all = predictions_all.merge(pred_rx[lkbkkeycols + ['pred_xxr_1',  'pred_xxr_2', 'pred_xxr_3']], on = lkbkkeycols, how = 'outer')

if not (med_i.shape[0] > 0) :
    predictions_all['pred_ixx_1'] = np.nan 
    predictions_all['pred_ixx_2'] = np.nan 
    predictions_all['pred_ixx_3'] = np.nan 
    predictions_all['ag_allow_ip_pmpm'] = 0

if not (med_o.shape[0] > 0):
    predictions_all['pred_xox_1'] = np.nan 
    predictions_all['pred_xox_2'] = np.nan 
    predictions_all['pred_xox_3'] = np.nan 
    predictions_all['ag_allow_op_pmpm'] = 0

if not rxvertdta.shape[0] > 0:
    predictions_all['pred_xxr_1'] = np.nan 
    predictions_all['pred_xxr_2'] = np.nan 
    predictions_all['pred_xxr_3'] = np.nan 
    predictions_all['ag_allow_rx_pmpm'] = 0

predictions_all['nan_xox'] =  predictions_all['pred_xox_1'].isna().astype(int) + predictions_all['pred_xox_2'].isna().astype(int) + predictions_all['pred_xox_3'].isna().astype(int)
predictions_all['nan_ixx'] =  predictions_all['pred_ixx_1'].isna().astype(int) + predictions_all['pred_ixx_2'].isna().astype(int) + predictions_all['pred_ixx_3'].isna().astype(int)
predictions_all['nan_xxr'] =  predictions_all['pred_xxr_1'].isna().astype(int) + predictions_all['pred_xxr_2'].isna().astype(int) + predictions_all['pred_xxr_3'].isna().astype(int)


predictions_all['pred_op_rx'] = xgb_op_rx.predict(np.array(predictions_all[[ 
                  'pred_xox_1', 'pred_xox_2', 'pred_xox_3', 
                   'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3',                                      
                                                                      ]]))

predictions_all['pred_op'] = xgb_op.predict(np.array(predictions_all[[ 
                  'pred_xox_1', 'pred_xox_2', 'pred_xox_3', 
                                                                      ]]))

predictions_all['pred_med'] = xgb_med.predict(np.array(predictions_all[[ 
                       'pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3',                                      
                       'pred_xox_1', 'pred_xox_2', 'pred_xox_3', 
                                                                      ]]))


predictions_all['pred_rx'] = xgb_rx.predict(np.array(predictions_all[[ 
                       'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3',                                      
                                                                      ]]))

predictions_all['pred_ior'] = xgb_ior.predict(np.array(predictions_all[[ 
                       'pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3',                                      
                       'pred_xox_1', 'pred_xox_2', 'pred_xox_3', 
                       'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3',                                      
                                                                      ]]))


predictions_all['ind_op_rx'] = ((predictions_all['nan_xox'] < 3) & (predictions_all['nan_ixx'] == 3)  & ( predictions_all['nan_xxr'] < 3 )).astype(int)
predictions_all['ind_op'] = ((predictions_all['nan_xox'] < 3) & (predictions_all['nan_ixx'] == 3)  & ( predictions_all['nan_xxr'] == 3 )).astype(int)
predictions_all['ind_rx'] =  ((predictions_all['nan_xox'] == 3) & (predictions_all['nan_ixx'] == 3)  & ( predictions_all['nan_xxr'] < 3 )).astype(int)
predictions_all['ind_med'] = ((predictions_all['nan_xox'] < 3) & (predictions_all['nan_ixx'] < 3)  & ( predictions_all['nan_xxr'] == 3 )).astype(int)
predictions_all['ind_ior'] =  1- predictions_all['ind_op_rx'] - predictions_all['ind_op']- predictions_all['ind_rx']  - predictions_all['ind_med']
predictions_all['ind_nan'] =  ((predictions_all['nan_xox'] == 3) & (predictions_all['nan_ixx'] == 3)  & ( predictions_all['nan_xxr'] == 3 )).astype(int)
predictions_all['ind_all'] =  ((predictions_all['nan_xox'] < 3) & (predictions_all['nan_ixx'] < 3)  & ( predictions_all['nan_xxr'] < 3 )).astype(int)


predictions_all['score'] = predictions_all['pred_op_rx'] * predictions_all['ind_op_rx'] +  predictions_all['pred_ior'] * predictions_all['ind_ior']                       +predictions_all['pred_op'] * predictions_all['ind_op'] + predictions_all['pred_rx'] * predictions_all['ind_rx']  +                      predictions_all['pred_med'] * predictions_all['ind_med']



predictions_all['ag_allow_op_pmpm'] = predictions_all['ag_allow_op_pmpm'].fillna(0)
predictions_all['ag_allow_ip_pmpm'] = predictions_all['ag_allow_ip_pmpm'].fillna(0)
predictions_all['ag_allow_rx_pmpm'] = predictions_all['ag_allow_rx_pmpm'].fillna(0)


# # Score on the group level 
# 

# In[ ]:



seg_dummy = pd.get_dummies(cust['bus_segment_cd'])
seg_dummy.columns = ['bus_segment_' + x for x in seg_dummy]
custdta0  = pd.concat([cust.drop('bus_segment_cd', axis = 1), seg_dummy], axis = 1)
custdta0['log_quoted_pfm_rel'] = np.log(custdta0['quoted_pfm_rel'] + 0.001)


# In[ ]:




#score_prediction
respdta = mbrrespdta.merge(predictions_all,on = lkbkkeycols, how = 'left')  

claims = respdta.groupby(by = grplkbkkey)['score', 'pred_xox_1','pred_xox_2', 'pred_xox_3', 'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3',
           'ag_allow_rx_pmpm', 'pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3', 'pred_ior', 'pred_op',
       'pred_op_rx', 'pred_rx', 'pred_med', ].agg({'mean', 'count'})
claims.columns = [f'{x}_{y}' for x,y in claims.columns]
claims=claims.reset_index()

grp_pred= respdta.groupby(grplkbkkey)['mbr_pers_gen_key'].count().reset_index()
grp_pred= grp_pred.rename(columns={"mbr_pers_gen_key": "counts_mbr"})

respdta['mths_est'] = 12 
mths_est= respdta.groupby(grplkbkkey)['mths_est'].sum().reset_index()

respdta['ag_allow_op'] = respdta['ag_allow_op_pmpm'] * respdta['mths_est'] 
respdta['ag_allow_ip'] = respdta['ag_allow_ip_pmpm'] * respdta['mths_est'] 
respdta['ag_allow_rx'] = respdta['ag_allow_rx_pmpm'] * respdta['mths_est'] 
pmpm_list_ttl = ['ag_allow_op','ag_allow_ip', 'ag_allow_rx' ]
grp_pmpms = respdta.groupby(grplkbkkey)[pmpm_list_ttl].sum().reset_index()

pmpm_list = ['ag_allow_op_pmpm', 'ag_allow_ip_pmpm', 'ag_allow_rx_pmpm']
grp_pmpms_avg = respdta.groupby(grplkbkkey)[pmpm_list].mean().reset_index()


respdta['any_xox'] = ( respdta['nan_xox'] <3)
respdta['any_ixx'] = (respdta['nan_ixx'] <3)
respdta['any_xxr'] = ( respdta['nan_xxr'] <3)
respdta['any_ior'] = 1- respdta['ind_nan']
num_list= ['any_xox', 'any_ixx', 'any_xxr', 'any_ior']

num_med =  respdta.groupby(grplkbkkey)[num_list].sum().reset_index()
#for i in num_list:
#    num_med[i] = (3-num_med[i])/prop_dict[i]

#merge the group level predictors 
custdta = custdta0.merge(grp_pred , on = grplkbkkey, how = 'left')
custdta = custdta.merge(mths_est , on = grplkbkkey, how = 'left')
custdta = custdta.merge(grp_pmpms , on =grplkbkkey , how = 'left')
custdta = custdta.merge(grp_pmpms_avg , on =grplkbkkey , how = 'left')
custdta = custdta.merge(claims, on = grplkbkkey, how = 'left')
#custdta = custdta.merge(claims_cnt, on = grplkbkkey, how = 'left')
#custdta = custdta.merge(ind_pct, on = grplkbkkey, how = 'left')
custdta = custdta.merge(num_med, on = grplkbkkey, how = 'left')


X_predict = custdta.copy().reindex(columns=cali_feature)    
X_predict =  sm.add_constant(X_predict, has_constant='add')

calibrated  = mod_wls.predict(X_predict) 
calibrated[calibrated < 0]  = 0 
custdta['offset'] = np.log(calibrated + 0.001)


custdta.rename(columns = {'score_mean': 'score'}, inplace = True)

cnt_columns = [x for x in custdta if x.endswith('count')]
for x in cnt_columns:
    custdta[x[:-5] + 'pct']  = custdta[x]/custdta['total_members']
for x in num_list:
    custdta[x]  = custdta[x]/custdta['total_members']

custdta[ 'bc_score' ] =  bc_trans(custdta['score' ], -0.2)
custdta[ 'log_score' ] =  bc_trans(custdta[ 'score' ], 0)

custdta['score_ttl'] = custdta[ 'score_pct' ] * custdta[ 'score' ]
custdta['log_score_pct'] = custdta['log_score'] * custdta[ 'score_pct' ]
custdta['bc_score_pct'] = custdta['bc_score'] * custdta[ 'score_pct' ]


custdta['year2016'] = 0
custdta['year2017'] = 0
custdta['year2018'] = 1

custdta['new_ind'] = 1

for i in pmpm_list_ttl:
    custdta[i] = custdta[i]/custdta['mths_est']

grp_features=['quoted_members', 'quoted_pfm_rel', 'comm_rt', 'lf_group',
   'bus_segment_S', 'bus_segment_T',
   'log_quoted_pfm_rel', 'counts_mbr', 'mths_est', 'ag_allow_op',
   'ag_allow_ip', 'ag_allow_rx', 'ag_allow_op_pmpm', 'ag_allow_ip_pmpm',
   'ag_allow_rx_pmpm', 'score', 'score_count', 'pred_xox_1_mean',
   'pred_xox_1_count', 'pred_xox_2_mean', 'pred_xox_2_count',
   'pred_xox_3_mean', 'pred_xox_3_count', 'pred_xxr_1_mean',
   'pred_xxr_1_count', 'pred_xxr_2_mean', 'pred_xxr_2_count',
   'pred_xxr_3_mean', 'pred_xxr_3_count', 'pred_ixx_1_mean',
   'pred_ixx_1_count', 'pred_ixx_2_mean', 'pred_ixx_2_count',
   'pred_ixx_3_mean', 'pred_ixx_3_count', 'pred_ior_mean',
   'pred_ior_count', 'pred_op_mean', 'pred_op_count', 'pred_op_rx_mean',
   'pred_op_rx_count', 'pred_rx_mean', 'pred_rx_count', 'pred_med_mean',
   'pred_med_count', 'offset', 'score_pct', 'pred_xox_1_pct',
   'pred_xox_2_pct', 'pred_xox_3_pct', 'pred_xxr_1_pct', 'pred_xxr_2_pct',
   'pred_xxr_3_pct', 'pred_ixx_1_pct', 'pred_ixx_2_pct', 'pred_ixx_3_pct',
   'pred_ior_pct', 'pred_op_pct', 'pred_op_rx_pct', 'pred_rx_pct',
   'pred_med_pct', 'bc_score', 'log_score', 'score_ttl', 'log_score_pct',
   'bc_score_pct','year2016', 'year2017', 'year2018', 'new_ind']

cols = list(grp_sampledta[grp_features])
#    custdta =  custdta.reindex(columns=cols)
x_grp = custdta.reindex(columns=cols)
weights =custdta['quoted_members']
pred = xgb_grp.predict(x_grp)


# In[ ]:



cust_pred = custdta[grplkbkkey + ['total_members', 'any_xox', 'any_ixx', 'any_xxr']]
cust_pred['any_ior'] =  custdta['any_ior']
cust_pred['pfm_calibrated'] = np.exp(custdta['offset'])
cust_pred['pred_risk'] = pred
cust_pred['risk_pfm'] = pred/(0.00001 +cust_pred['pfm_calibrated'])

#join in rx and med debits
cust_pred = (cust_pred.merge(custdta[['src_rpt_cust_id', 'underwriting_date',
                                     'rx_debits', 'med_debits']], how = 'left',
                           on = ['src_rpt_cust_id', 'underwriting_date'])
                      .fillna(0))
cust_pred['rx_debit_rs']=cust_pred['rx_debits']/(12*cust_pred['total_members']*cust_pred['pfm_calibrated'])
cust_pred['med_debit_rs']=cust_pred['med_debits']/(12*cust_pred['total_members']*cust_pred['pfm_calibrated'])
cust_pred = cust_pred.drop(['total_members', 'rx_debits','med_debits'],axis=1)
