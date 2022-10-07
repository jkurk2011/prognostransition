#!/usr/bin/env python
# coding: utf-8

# In[1]:




import numpy as np
from numpy import isnan, log
from numpy import log
from numpy import isnan

import pandas as pd
from pandas import datetime

import time
import os 
import datetime
from datetime import date
import xgboost as xgb
import lightgbm
import matplotlib.pyplot as plt
import math


from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
from sklearn.feature_extraction.text import TfidfVectorizer, CountVectorizer
from sklearn.metrics import mean_squared_error
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score
from sklearn.externals import joblib 

import statsmodels.api as sm
from statsmodels.sandbox.regression.predstd import wls_prediction_std 
from statsmodels.discrete.discrete_model import Poisson

import sqlite3
import pandasql
from pandasql import sqldf
pyqldf = lambda q: sqldf(q, globals())
import seaborn as sns
import pickle

import warnings
warnings.filterwarnings('ignore')


# In[2]:


#set directory path 
os.chdir ('/advanalytics/pgk0233/DRG_20.08/sourcedprograms') #needed for %run statements
datapath = '/advanalytics/pgk0233/IQVIA_Score/inputdata/' #fetch raw input data from IQVIA or HRAE submission
modelpath = '/advanalytics/pgk0233/IQVIA_Score/modelobjects/' # fetch save model 
scorepath = '/advanalytics/pgk0233/DRG_20.08/outputscores/' # location to save output tables 
mappath = '/advanalytics/pgk0233/mappingfiles/'  # fetch lookup tables for the medical/rx recording 


# In[3]:


#load the dependency models 
get_ipython().run_line_magic('run', "-i 'load_models_and_lookuptables.py'")
get_ipython().run_line_magic('run', "-i 'load_queries.py'")


# ## Use this code to run a single group

# In[4]:


#Submitted Census info
iqsubmithead = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/AccountManager_aecf085a_GRx_CensusFile.txt', sep="|",
                           header=None,
                           usecols=[0,2,3,4,5,6],
                           names=['recordtype','group_id','group_name','effective_date','group_state','group_zip'],
                           nrows=1,
                          parse_dates=['effective_date']
                          )
iqsubmittail = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/AccountManager_aecf085a_GRx_CensusFile.txt', sep="|",
                           header = None,
                           skiprows=1,
                          names=['recordtype','pers_first_name','pers_last+name','pers_zip_cd','pers_gender','pers_birth_date'],
                          parse_dates=['pers_birth_date']
                          )


# In[24]:


#Return info
#request all: 340_aecf085a_202001311430_xx.txt
#op and rx: 320_aecf085a_202001311430_xx.txt
#rx only 300_aecf085a_202001311430_rx.txt
try: 
    del (rawmeddta)
    del (rawrxdta)
except:
    1
    
rawmeddta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/240_STUDY_202004091342_DX.TXT', sep="|",
                       parse_dates = ['FROMDATE'])
# rawmeddta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/samplegroup/240_STUDY_202004091342_DX.TXT', sep="|",
#                    parse_dates = ['FROMDATE'])
rawrxdta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/240_STUDY_202004091342_RX.TXT', 
                       sep="|")
# rawrxdta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/samplegroup/240_STUDY_202004091342_RX.TXT', 
#                        sep="|")

# try:
#     rawmeddta
# except:
#     rawmeddta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/rawmedheader.txt', sep="|")
# try:
#     rawrxdta
# except:
#     rawrxdta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/samplegroup/rawrxheader.txt',sep="|")
    
for name in [iqsubmithead, iqsubmittail, rawmeddta, rawrxdta]:
    name.columns = [x.lower() for x in name]


# In[25]:


#Run the model on one group

meddcoverride = 0
meddcoverrideamt = 10000
rxdcoverride = 0
rxdcoverrideamt = 1000
first_interval = 3

# keep_i = False
# keep_o = False
# keep_r = True

get_ipython().run_line_magic('run', "-i 'transformdata_singlegroup_s1.py'")
get_ipython().run_line_magic('run', "-i 'transformdata_s2.py'")
get_ipython().run_line_magic('run', "-i 'score_models.py'")


# ## Use this code to run a batch of groups

# In[ ]:


# run entire POC dataset without "debiting" for specialty Rx

meddcoverride = 0
meddcoverrideamt = 0
rxdcoverride = 0
rxdcoverrideamt = 0
first_interval = 3

keep_i = True
keep_o = True
keep_r = True
# is_drg = 0

rawmeddta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/Data/L2569UPA.246_DX.txt', sep="|"
#                            ,nrows =1000
                       )


rawrxdta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/Data/L2569UPA.246_RX.txt', sep="|"
#                         ,nrows = 1000
                      )

# rawmeddta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/data/drg_medical_final_v2.csv'
#                            ,nrows =1000
#                        )


# rawrxdta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/data/drg_rx_final_v2.csv'
#                         ,nrows = 1000
#                       )


get_ipython().run_line_magic('run', "-i 'transformdata_batchpoc_s1.py'")
get_ipython().run_line_magic('run', "-i 'transformdata_s2.py'")
get_ipython().run_line_magic('run', "-i 'score_models.py'")

qtr_rxdeb.to_csv(scorepath + '20205010iq_rx_debits_join.csv')
cust_pred.to_csv(scorepath + '20200510iqCustpred_IOR.csv')

# qtr_meddeb.to_csv(scorepath + '2020041qtr_meddeb.csv')
# diagout = medvertdta[(medvertdta.times==1)  & (medvertdta.datatype=='md')]
# diagout.to_csv(scorepath + '20200417diagout.csv')
# mbrrespdta.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/20200322iqpreddebit_IOR.csv')
# qtr_meddeb.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/iq_rxspc.csv')
# qtr_rxdeb.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/iq_medspc.csv')


# In[ ]:


meddcoverride = 0
meddcoverrideamt = 0
rxdcoverride = 0
rxdcoverrideamt = 0
first_interval = 3

keep_i = True
keep_o = True
keep_r = True
# is_drg = 0

# rawmeddta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/Data/L2569UPA.246_DX.txt', sep="|"
#                            ,nrows =1000
#                        )


# rawrxdta = pd.read_csv('/advanalytics/pgk0233/IQVIA_Score/Data/L2569UPA.246_RX.txt', sep="|"
#                         ,nrows = 1000
#                       )

rawmeddta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/data/drg_medical_final_v2.csv'
#                            ,nrows =1000
                       )


rawrxdta = pd.read_csv('/advanalytics/pgk0233/DRG_20.08/data/drg_rx_final_v2.csv'
#                         ,nrows = 1000
                      )


get_ipython().run_line_magic('run', "-i 'transformdata_batchpoc_s1.py'")
get_ipython().run_line_magic('run', "-i 'transformdata_s2.py'")
get_ipython().run_line_magic('run', "-i 'score_models.py'")

qtr_rxdeb.to_csv(scorepath + '20205010drg_rx_debits_join.csv')
cust_pred.to_csv(scorepath + '20200510drgCustpred_IOR.csv')

# qtr_meddeb.to_csv(scorepath + '2020041qtr_meddeb.csv')
# diagout = medvertdta[(medvertdta.times==1)  & (medvertdta.datatype=='md')]
# diagout.to_csv(scorepath + '20200417diagout.csv')
# mbrrespdta.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/20200322iqpreddebit_IOR.csv')
# qtr_meddeb.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/iq_rxspc.csv')
# qtr_rxdeb.to_csv('/advanalytics/pgk0233/IQVIA_Score/Data/iq_medspc.csv')


# In[49]:


cust_pred


# In[6]:




