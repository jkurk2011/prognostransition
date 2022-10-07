#!/usr/bin/env python
# coding: utf-8

###################################SIMPLIFIED MANUAL RATE LOOKUP TABLE#################################
pfm = '''
select 
    a.*,
    pfm.age_gender_factor
from iqsubmit a
left join pfmsimple pfm
on a.pers_gender = pfm.sex_cd
and a.subscriber_flag = pfm.subscriber_ind
and a.age between pfm.min_mbr_age and pfm.max_mbr_age
'''

###################################DIAGNOSIS MAPPING TO HCUP ##################################
dq = '''
select
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    case 
        when hcup9.diag_cd is not null then hcup9.hcup
        else hcup10.hcup 
    end as value,
    '' as ndc_id,
    'md' as datatype,
    count(*) as occurrences,
    0 as units_per_occurrence,
    0 as charge_amt,
    0 as ag_allow_amt
from diagvert a
left join icd09lookup hcup9
on a.value = hcup9.diag_cd
left join icd10lookup hcup10
on a.value = hcup10.diag_cd
group  by
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    case 
        when hcup9.diag_cd is not null then hcup9.hcup
        else hcup10.hcup 
    end
'''

###################################CPT AND NDC CODE DATA ##################################
qmc = '''
select 
    a.*,
    chargedsign*(cpt.allowed_per_unit)
    *(case when procunits > cpt.serv_unit_cnt_max then procunits else cpt.serv_unit_cnt_max end) 
    as cpt_allowed,
    chargedsign*(medndc.allowed_per_unit)
    *(case when procunits > medndc.serv_unit_cnt_max then procunits else medndc.serv_unit_cnt_max end) 
    as ndc_allowed,
    coalesce(n.milliman_match_name,h.milliman_match_name) as milliman_match_name,
    coalesce(n.debits,h.debits) as debits,
    coalesce(n.plan_yr_dollar_assumption,h.plan_yr_dollar_assumption) as plan_yr_dollar_assumption,
    coalesce(n.earliest_period,h.earliest_period) as earliest_period,
    coalesce(n.latest_period,h.latest_period) as latest_period
from qtr_meddta_mc a
left join cptagnostic cpt
on a.cpt_cd = cpt.cpt_cd
left join medndcagnostic medndc
on a.ndc_id = medndc.ndc_id
LEFT JOIN hrdl_hcpcs_debits h
ON a.cpt_cd = h.hcpcs
LEFT JOIN hrdl_ndc_debits n
ON a.ndc_id = n.ndc_id
;
'''

q2mc = '''
select 
    a.group_id,
    a.src_rpt_cust_id,
    a.mbr_pers_gen_key,
    a.underwriting_date,
    a.times,
    a.med_sample,
    case 
        when gen.symp is not null then '99999' 
        else a.cpt_cd 
    end as value,
    a.ndc_id,
    'mc' as datatype,
    count(*) as occurrences,
    sum(a.procunits)/count(*) as units_per_occurrence,
    sum(a.charged) as charge_amt,
    sum(case
       when debcost is not null then debcost
       when coalesce(a.ndc_allowed, a.cpt_allowed,a.charged, 0 )>a.charged then charged
       else coalesce(a.ndc_allowed, a.cpt_allowed,a.charged, 0 )
    end) as ag_allow_amt
from iq_cptvert a
left join cptgenetic gen
on a.cpt_cd = gen.symp
left join debjoin3 dj
on a.mbr_pers_gen_key = dj.mbr_pers_gen_key
and a.src_rpt_cust_id = dj.src_rpt_cust_id
and a.group_id = dj.group_id
and a.underwriting_date = dj.underwriting_date
and a.times = dj.times
and a.med_sample = dj.med_sample
and a.cpt_cd = dj.cpt_cd
and a.ndc_id = dj.ndc_id
group by 
    a.group_id,
    a.src_rpt_cust_id,
    a.mbr_pers_gen_key,
    a.underwriting_date,
    a.times,
    a.med_sample,
    case 
        when gen.symp is not null then '99999' 
        else a.cpt_cd 
    end,
    a.ndc_id
;
'''
###################################REVENUE CODE DATA WHERE CLAIMS ARE COUNTED##################################
qmr = '''
select 
    a.*,
    chargedsign*(rev.allowed_per_unit)
    *(case when procunits > rev.serv_unit_cnt_max then procunits else rev.serv_unit_cnt_max end) 
    as rev_allowed
from qtr_meddta_mr a
left join revagnostic rev
on a.revenue_cd = rev.revenue_cd
;
'''

q2mr = '''
select 
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    revenue_cd as value,
    '' as ndc_id,
    'mr' as datatype,
    count(*) as occurrences,
    sum(procunits)/count(*) as units_per_occurrence,
    sum(charged) as charge_amt,
    sum(case
       when coalesce(rev_allowed,charged, 0 )>charged then charged
       else coalesce(rev_allowed,charged, 0 )
    end) as ag_allow_amt
from iq_revvert a
group by 
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    revenue_cd
;
'''
###################################REVENUE CODE DATA WHERE CLAIMS NOT COUNTED##################################

q2mr2 = '''
select 
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    revenue_cd as value,
    '' as ndc_id,
    'mr' as datatype,
    0 as occurrences,
    0 as units_per_occurrence,
    0 as charge_amt,
    0 as ag_allow_amt
from qtr_meddta_mr2 a
group by 
    group_id,
    src_rpt_cust_id,
    mbr_pers_gen_key,
    underwriting_date,
    times,
    med_sample,
    revenue_cd
;
'''

###########################################PREP RX DATA #####################################
r2 = '''
select 
    a.group_id,
    a.src_rpt_cust_id,
    a.mbr_pers_gen_key,
    a.underwriting_date,
    a.times,
    a.ndc_id,  
    'r' as rx_sample,
    'rg' as datatype,
    count(*) as occurrences,
    sum(days_supply_cnt)/count(*) as units_per_occurrence,
    sum(allowed) as allow_amt,
    sum(coalesce(debcost, ag_allow)) as ag_allow_amt
from qtr_rxdta a
left join rxdebjoin3 dj
on a.mbr_pers_gen_key = dj.mbr_pers_gen_key
and a.src_rpt_cust_id = dj.src_rpt_cust_id
and a.group_id = dj.group_id
and a.underwriting_date = dj.underwriting_date
and a.times = dj.times
and a.ndc_id = dj.ndc_id
group by 
    a.group_id,
    a.src_rpt_cust_id,
    a.mbr_pers_gen_key,
    a.underwriting_date,
    a.times,
    a.ndc_id
;
'''

####################CREATE TABLE OF MEDICAL SPECIALTY DRUGS#####################################
m = '''
select
	a.qtr_from_plnstart,
	a.mbr_pers_gen_key,
	a.underwriting_date,
	a.cpt_cd as hcpcs,
	a.ndc_id,
    a.group_id,
    a.src_rpt_cust_id,
	a.med_sample as sample,
	max(coalesce(n.milliman_match_name,h.milliman_match_name)) as milliman_match_name,
    count(*) as occurrences,
    sum(procunits)/count(*) as units_per_occurrence,
    sum(charged) as charge_amt,
    sum(case
       when coalesce(ndc_allowed, cpt_allowed,charged, 0 )>charged then charged
       else coalesce(ndc_allowed, cpt_allowed,charged, 0 )
    end) as ag_allow_amt,
	max(coalesce(n.debits,h.debits,0)) as debits,
	max(n.debits) as ndc_debits,
	max(h.debits) as hcup_debits,
	max(coalesce(n.minimum_charge ,h.minimum_charge ,0)) as minimum_charge,
	max(n.minimum_charge) as ndc_minimum_charge,
	max(h.minimum_charge) as hcup_minimum_charge
FROM iq_cptvert a
LEFT JOIN hrdl_hcpcs_debits h
ON a.cpt_cd = h.hcpcs
LEFT JOIN hrdl_ndc_debits n
ON a.ndc_id = n.ndc_id
WHERE coalesce(n.ndc_id,h.hcpcs) is not null
GROUP BY
	a.qtr_from_plnstart,
	a.mbr_pers_gen_key,
	a.underwriting_date,
	a.cpt_cd,
	a.ndc_id,
    a.group_id,
    a.src_rpt_cust_id,
	a.med_sample
;
'''

####################CREATE TABLE OF RX SPECIALTY DRUGS#####################################
r = '''
select
	a.qtr_from_plnstart,
	a.mbr_pers_gen_key,
	a.underwriting_date,
	a.ndc_id,
    a.group_id,
    a.src_rpt_cust_id,
	max(d.milliman_match_name) as milliman_match_name,
    count(*) as occurrences,
    sum(days_supply_cnt)/count(*) as units_per_occurrence,
    sum(allowed) as allow_amt,
    sum(ag_allow) as ag_allow_amt,
	max(d.debits) as debits,
	max(d.minimum_charge) as minimum_charge
FROM qtr_rxdta a
INNER JOIN hrdl_ndc_debits d
ON a.ndc_id = d.ndc_id
GROUP BY
	a.qtr_from_plnstart,
	a.mbr_pers_gen_key,
    a.group_id,
    a.src_rpt_cust_id,
	a.underwriting_date,
	a.ndc_id
;
'''
