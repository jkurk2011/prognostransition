# -*- coding: utf-8 -*-
"""
Created on Tue Jan 12 17:00:44 2021

@author: EXJ5730
"""

# -*- coding: utf-8 -*-
"""
Created on Tue Jan  5 12:09:02 2021

@author: EXJ5730
"""

HIDDEN_PLACEHOLDER = 'Cannot Disclose'
ROUND_DECIMALS = 4
MIN_ADULT_AGE = 18

""" Utility functions"""
import numpy as np
import pandas as pd

 
def get_calendar_month_diff(date_start, date_end):
    """Calculate difference in calendar months"""
    return 12 * (date_end.dt.year - date_start.dt.year) + \
           (date_end.dt.month - date_start.dt.month) + \
           (date_end.dt.day - date_start.dt.day) \
               .apply(lambda x: np.timedelta64(x, 'D')) / np.timedelta64(1, 'M')


def get_fixed_month_diff(date_start_column, date_end_column):
    """Calculate difference in 30-days months"""
    return (date_end_column - date_start_column) / 30


def make_column_names_lowercase(df: pd.DataFrame) -> None:
    """Makes all column names in DataFrame lowercase."""
    df.columns = df.columns.str.lower()


def get_age_sex_stats(iqsubmit: pd.DataFrame) -> (int, int, int):
    """Count children and male and female adults"""
    is_adult = iqsubmit.age >= MIN_ADULT_AGE
    is_male = iqsubmit.pers_gender == 'M'
    is_female = iqsubmit.pers_gender == 'F'
    male_adults = (is_adult & is_male).sum()
    female_adults = (is_adult & is_female).sum()
    children = (~is_adult).sum()
    return male_adults, female_adults, children


def fill_empty_display_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Substitute missing values for 'Display' flag and its related columns.
    Default values are used.
    Args:
        df: source DataFrame
    Returns:
        DataFrame with substituted values
    """
    values = {"display_flag": "Y", "adult_male": 0, "adult_female": 0, "children": 0}
    return df.fillna(value=values)




#----------------------------------------------------------------------------------------------------
    

#----------------------------------------------------------------------
#- Newborn method loading

#---- readers

def read_smart_diagnosis_rules(path: str) -> pd.DataFrame:
    """Read smart diagnosis table"""
    smart_diagnosis = pd.read_csv(path,
                                  dtype={'Event cost': str,
                                         'CPT_Signal': str,
                                         'Dx_Signal': str,
                                         "adult_male": pd.Int64Dtype(),
                                         "adult_female": pd.Int64Dtype(),
                                         "children": pd.Int64Dtype()},
                                  index_col=False)
    make_column_names_lowercase(smart_diagnosis)
    smart_diagnosis['event cost'] = smart_diagnosis['event cost'].str.replace(',', '').astype(float)
    smart_diagnosis.dropna(how='all', inplace=True)
    smart_diagnosis = fill_empty_display_flags(smart_diagnosis)
    return smart_diagnosis

#--------------------------
#--- transforms


""" Utility functions used for data transformation and newborn claims prediction"""

DIAGNOSIS_COLUMN_PREFIX = 'diag'


def add_missing_columns(dx_data, cust, iqsubmit):
    """Add underwriting date, effective date, total_members to dx_data

    Args:
        dx_data: Dx pandas DataFrame
        cust: intermediate DataFrame from production pipeline
        iqsubmit: intermediate DataFrame from production pipeline
    """
    cust = cust[['group_id', 'underwriting_date', 'total_members']]
    dx_data = pd.merge(dx_data,
                       iqsubmit[['group_id', 'effective_date']],
                       how='inner', on='group_id')
    dx_data = pd.merge(dx_data, cust, how='inner',
                       on='group_id')
    dx_data[['underwriting_date', 'effective_date']] = dx_data[['underwriting_date',
                                                                'effective_date']] \
        .apply(pd.to_datetime)
    return dx_data


def transform_dx_data(dx_data):
    """Transform Dx data

    Args:
        dx_data: Dx pandas DataFrame (med data)
    Returns:
        transformed pandas DataFrame
    """
    dx_data['fromdate'] = pd.to_datetime(dx_data['fromdate']).astype('<M8[D]')
    return dx_data


def unpivot_diagnosis(dx_data):
    """Unpivot diagnosis: create a separate row per diagnosis code for each claim"""
    return pd.melt(dx_data,
                   id_vars=[col for col in dx_data.columns
                            if not col.startswith(DIAGNOSIS_COLUMN_PREFIX)],
                   value_name='diag') \
        .drop(['variable'], axis=1)


def get_latest_signals(dx_data, smart_diagnosis_filtered):
    """Select the latest date for each signal

    Args:
        dx_data: Dx pandas DataFrame (med data) with unpivoted diagnosis codes
        smart_diagnosis_filtered: filtered smart diagnosis pandas DataFrame containing signal likelihoods
    """

    # consider claims in dx_data where signal date (fromdate) < underwriting date
    dx_data = dx_data[dx_data.fromdate < dx_data.underwriting_date]
    merge_proc_diag = dx_data.merge(smart_diagnosis_filtered,
                                    left_on=['proccode', 'diag'],
                                    right_on=['cpt_signal', 'dx_signal'], how='inner')
    merge_diag_only = dx_data.merge(smart_diagnosis_filtered[smart_diagnosis_filtered.cpt_signal.isna()],
                                    left_on='diag',
                                    right_on='dx_signal', how='inner')
    merge_proc_only = dx_data.merge(smart_diagnosis_filtered[smart_diagnosis_filtered.dx_signal.isna()],
                                    left_on='proccode',
                                    right_on='cpt_signal', how='inner')
    all_signals = pd.concat([merge_proc_diag, merge_diag_only, merge_proc_only], ignore_index=True)
    all_signals['cpt_signal'].fillna('', inplace=True)
    all_signals['dx_signal'].fillna('', inplace=True)
    grouping_features = ['event', 'underwriting_date',
                         'group_id', 'mbr_pers_gen_key',
                         'cpt_signal', 'dx_signal']
    last_signal_data = all_signals.loc[all_signals.groupby(grouping_features)['fromdate'].idxmax()]
    return last_signal_data

#----------------------------------
# newborn claims calculations

LAST_LIKELIHOOD_MONTH = 17
COVERAGE_DURATION = 12
EPS = 1e-5


def get_plan_likelihood(data, weight_partial=False):
    """Sum likelihoods after the effective date

    Args:
        data: input pandas DataFrame
        weight_partial: if True, use uniform distribution assumption
            for a partially covered month interval
    Returns:
        aggregated likelihood column
    """
    return sum([data[f'{i}-{i + 1} month'] *
                (np.minimum(1, np.maximum(0, i + 1 - data['months_since_signal'])) *
                 np.minimum(1, np.maximum(0, data['max_plan_month'] - i))
                 if weight_partial else
                 (i + 1 > data['months_since_signal']) *
                 (data['max_plan_month'] > i))
                for i in range(LAST_LIKELIHOOD_MONTH)] +
               [data[f'{LAST_LIKELIHOOD_MONTH}+ month'] *
                (data['max_plan_month'] > LAST_LIKELIHOOD_MONTH)])


def predict_member_newborn_claims(dx_data, cust, iqsubmithead, smart_diagnosis,
                                  weight_partial=False):
    """Predict member-level newborn costs

    Args:
        dx_data: Dx pandas DataFrame
        cust: intermediate DataFrame from production pipeline
        iqsubmithead: census data head
        smart_diagnosis: smart diagnosis pandas DataFrame
            use utils.readers.read_smart_diagnosis_rules for table loading
        weight_partial: if True, use uniform distribution assumption
            for a partially covered month interval
    Returns:
        estimated member-level newborn costs pandas DataFrame
    """
    dx_data = transform_dx_data(dx_data)

    # XXX: replace this logic if the business rules are updated
    dx_data = add_missing_columns(dx_data, cust, iqsubmithead)

    dx_data = unpivot_diagnosis(dx_data)
    last_signal_data = get_latest_signals(dx_data, smart_diagnosis)
    last_signal_data['months_since_signal'] = get_calendar_month_diff(last_signal_data['fromdate'],
                                                                      last_signal_data['effective_date'])
    last_signal_data['max_plan_month'] = last_signal_data['months_since_signal'] + COVERAGE_DURATION
    last_signal_data['event_likelihood'] = get_plan_likelihood(last_signal_data, weight_partial)
    last_signal_data['event_est_cost_amt'] = last_signal_data['event cost'] * \
                                             last_signal_data['event_likelihood']
    member_newborn_claims = last_signal_data.loc[
        last_signal_data.groupby(['event', 'underwriting_date', 'group_id', 'mbr_pers_gen_key'])[
            'event_est_cost_amt'].idxmax()]

    return member_newborn_claims


def aggregate_member_level_claims(member_newborn_claims, cust_pred=None, manual_rate=350):
    """Aggregate member-level claims into group claims

    Args:
        member_newborn_claims: predicted member-level claims pandas DataFrame
                               (output of predict_member_newborn_claims)
        cust_pred: resulting DataFrame from production pipeline
        manual_rate: manual rate (constant numeric value used
            if cust_pred is not provided)
    """
    grouping_features = ['event', 'underwriting_date', 'group_id']
    group_newborn_claims = member_newborn_claims.groupby(grouping_features) \
        .agg({'event_est_cost_amt': sum, 'total_members': max}).reset_index()
    cust_pred = cust_pred[['src_rpt_cust_id', 'underwriting_date', 'any_xox', 'any_ixx', 'any_xxr',
                           'any_ior', 'pfm_calibrated', 'pred_risk', 'risk_pfm', 'rx_debit_rs']]
    group_newborn_risks = group_newborn_claims.merge(cust_pred,
                                                     left_on=['group_id', 'underwriting_date'],
                                                     right_on=['src_rpt_cust_id', 'underwriting_date'],
                                                     how='inner')
    group_newborn_risks = group_newborn_risks.rename(columns={'event_est_cost_amt': 'event_med_debits'})
    if cust_pred is None:
        annual_member_manual_costs = 12 * manual_rate * group_newborn_risks['total_members']
    else:
        annual_member_manual_costs = 12 * group_newborn_risks['pfm_calibrated'] * \
                                     group_newborn_risks['total_members']
    group_newborn_risks['event_med_debit_rs'] = group_newborn_risks['event_med_debits'] / \
                                          (annual_member_manual_costs + EPS)
    return group_newborn_risks


def get_group_risk(group_newborn_risks, manual_rate=350):
    grouping_features = ['underwriting_date', 'group_id']
    if ('pfm_calibrated' in group_newborn_risks.columns):
        group_med_debit = group_newborn_risks.groupby(grouping_features) \
            .agg({'event_med_debits': sum, 'total_members': max, 'pfm_calibrated': max}).reset_index()
        annual_group_costs = 12 * group_med_debit['pfm_calibrated'] * group_med_debit['total_members']
    else:
        group_med_debit = group_newborn_risks.groupby(grouping_features) \
            .agg({'event_med_debits': sum, 'total_members': max}).reset_index()
        annual_group_costs = 12 * manual_rate * group_med_debit['total_members']

    group_med_debit = group_med_debit.rename(columns={'event_med_debits': 'med_debits'})
    group_med_debit['med_debit_rs'] = group_med_debit['med_debits'] / \
                                      (annual_group_costs + EPS)
    return group_med_debit


def filter_smart_diagnosis_table(smart_diagnosis, iqsubmit):
    """Filter smart_diagnosis_table"""
    male_adults, female_adults, children = get_age_sex_stats(iqsubmit)

    smart_diagnosis_filtered = smart_diagnosis
    smart_diagnosis_filtered['event grouping'] = smart_diagnosis_filtered['event']
    smart_diagnosis_filtered['signal description grouping'] = smart_diagnosis_filtered['signal description']
    for x in ['event grouping', 'signal description grouping']:
        smart_diagnosis_filtered.loc[(smart_diagnosis_filtered['adult_male'] > male_adults) |
                                     (smart_diagnosis_filtered['adult_female'] > female_adults) |
                                     (smart_diagnosis_filtered['children'] > children) |
                                     (smart_diagnosis_filtered['display_flag'] == "N"), x] = HIDDEN_PLACEHOLDER

    return smart_diagnosis_filtered


def get_imminent_treatment_table(member_newborn_claims, group_newborn_risks):
    """Construct imminent treatment table

    Args:
        member_newborn_claims: predicted member-level claims pandas DataFrame
            (output of predict_member_newborn_claims)
        group_newborn_risks: predicted group-level claims pandas DataFrame
            (output of predict_group_newborn_claims)
    """
    if member_newborn_claims.empty:
        imm_table = pd.DataFrame(columns = ['Events', 'Signal Date Range', 'Full Event Cost', 'Probability Weighted Cost', 'Impact to Group Risk Score', 'Signal Description', 'cpt_signal', 'dx_signal'])
    else:
        imm_table = member_newborn_claims.merge(group_newborn_risks,
                                            on=['event', 'group_id', 'underwriting_date'],
                                            suffixes=('', '_group'))
        annual_member_manual_costs = 12 * imm_table['pfm_calibrated'] * imm_table['total_members']
        imm_table['impact'] = imm_table['event_est_cost_amt'] / (annual_member_manual_costs + EPS)
        imm_table['impact'] = imm_table['impact'].round(ROUND_DECIMALS)
        imm_table["signal_detected"] = get_calendar_month_diff(
                imm_table['fromdate'], imm_table['underwriting_date']).apply(map_months_to_range)
        output_columns = ['event grouping',
                      'signal description grouping',
                      'fromdate',
                      'signal_detected',
                      'event cost',
                      'event_est_cost_amt',
                      'impact']
        imm_table = imm_table.sort_values(['underwriting_date', 'event_est_cost_amt'], ascending=False)
        imm_table = imm_table[output_columns] \
            .rename(columns={'event grouping': 'Events',
                             'signal description grouping': 'Signal Description',
                             'fromdate': 'Signal Date',
                             'signal_detected': 'Signal Date Range',
                             'event cost': 'Full Event Cost',
                             'event_est_cost_amt': 'Probability Weighted Cost',
                             'impact': 'Impact to Group Risk Score'})
        #imm_table = imm_table[['Events','Signal Date Range','Full Event Cost','Probability Weighted Cost','Impact to Group Risk Score','Signal Description','cpt_signal','dx_signal']]
        #imm_table = imm_table[(imm_table['Probability Weighted Cost']>=1000) | (imm_table['Impact to Group Risk Score']>= 0.01)]

    return imm_table
    

#-----------------------------------------------------------------------------------------------------
    

"""Reader functions for census, rx, dx, hcup_prev and prod_table."""


def read_hcup_prev_table(path: str) -> pd.DataFrame:
    """Read hcup_prev table.
    Args:
        path: str - path to folder
    Return:
        hcup_prev: pandas DataFrame
    """
    hcup_prev = pd.read_csv(path,
                            index_col=False)
    hcup_prev = hcup_prev.dropna(subset=["value"])
    make_column_names_lowercase(hcup_prev)
    hcup_prev = fill_empty_display_flags(hcup_prev)
    return hcup_prev


def read_dx_data(path: str) -> pd.DataFrame:
    """Read dx_data table for claims_factors.
    Args:
        path: str - path to folder
    Return:
        dx_data: pandas DataFrame
    """
    dx_data = pd.read_csv(path,
                          sep="|",
                          low_memory=False,
                          dtype={"NDC_CD": pd.Int64Dtype(),
                                 "PROCCODE": str},
                          parse_dates=["DATA_THRU"],
                          date_parser=lambda x: pd.to_datetime(x, format="%Y%m"),
                          index_col=False)
    make_column_names_lowercase(dx_data)
    return dx_data


def read_rx_data(path: str) -> pd.DataFrame:
    """Read rx_data table for claims_factors.
    Args:
        path: str - path to folder
    Return:
        rx_data: pandas DataFrame
    """
    rx_data = pd.read_csv(path,
                          sep="|",
                          low_memory=False,
                          dtype={"NDC_CODE": pd.Int64Dtype()},
                          index_col=False)
    make_column_names_lowercase(rx_data)
    return rx_data


def read_census_data(path: str) -> (pd.DataFrame, pd.DataFrame):
    """Read census table for claims_factors.
    Args:
        path: str - path to folder
    Return:
        iqsubmithead, iqsubmittail: pandas DataFrame
    """
    iqsubmithead = pd.read_csv(path,
                               sep="|",
                               header=None,
                               usecols=[0, 2, 3, 4, 5, 6],
                               names=['recordtype',
                                      'group_id',
                                      'group_name',
                                      'effective_date',
                                      'group_state',
                                      'group_zip'],
                               nrows=1,
                               parse_dates=['effective_date'],
                               index_col=False)
    make_column_names_lowercase(iqsubmithead)
    iqsubmittail = pd.read_csv(path,
                               sep="|",
                               header=None,
                               skiprows=1,
                               names=['recordtype',
                                      'pers_first_name',
                                      'pers_last_name',
                                      'pers_zip_cd',
                                      'pers_gender',
                                      'pers_birth_date'],
                               parse_dates=['pers_birth_date'],
                               index_col=False)
    make_column_names_lowercase(iqsubmittail)
    return iqsubmithead, iqsubmittail


#------------------------------------------------------------------------------------------------------
#-- transforms

FIRST_RANGE_THRESHOLD = 4
SECOND_RANGE_THRESHOLD = 12


def map_months_to_range(months_from_uw):
    if months_from_uw < FIRST_RANGE_THRESHOLD:
        return "0-3 Months to UW"
    if months_from_uw < SECOND_RANGE_THRESHOLD:
        return "4-12 Months to UW"
    return "13+ Months to UW"
    
"""Baseline Smart Specialty table processing logic"""


def filter_prod_info(hrdl_debits_clean, male_adults, female_adults, children):
    """Filter drugs lookup table hrdl_debits_clean
    Args:
        hrdl_debits_clean: intermediate pandas DataFrame, original lookup prod_table
        male_adults: count of male adults in a group, according census file
        female_adults: count of female adults in a group, according census file
        children: count of children in a group, according census file
    Returns:
        DataFrame grouped by milliman_match_name and display rules"""
    prod_filtered = hrdl_debits_clean
    prod_filtered = fill_empty_display_flags(prod_filtered)
    prod_filtered = prod_filtered.astype({'plan_yr_dollar_assumption': "float64"})
    prod_filtered['display_flag'] = prod_filtered['display_flag'].apply(lambda x: 1 if x == 'N' else 0)
    prod_filtered = prod_filtered.groupby(['milliman_match_name']).agg({'adult_male': max,
                                                                        'adult_female': max,
                                                                        'children': max,
                                                                        'display_flag': sum}).reset_index()
    prod_filtered['prod grouping'] = prod_filtered['milliman_match_name']
    prod_filtered.loc[(prod_filtered['adult_male'] > male_adults) |
                      (prod_filtered['adult_female'] > female_adults) |
                      (prod_filtered['children'] > children) |
                      (prod_filtered['display_flag'] > 0), 'prod grouping'] = HIDDEN_PLACEHOLDER

    return prod_filtered


def get_smart_specialty_periods(qtr_rxdeb, qtr_meddeb):
    """Predict First and Last Detected for rx and dx claims by group, member, drug

        Args:
            qtr_rxdeb: intermediate DataFrame from production pipeline;
                       contains group and member ids, rx claims data (in times = 1,2,3),
                       enriched with agnostic plan year assumption values and
                       earliest period, latest period values from ndc mapping table
            qtr_meddeb: intermediate DataFrame from production pipeline;
                        contains group and member ids, med (dx) claims data (in times = 1,2,3),
                        enriched with agnostic plan year assumption values and
                        earliest period, latest period values from ndc mapping table
        Returns:
            appended pandas DataFrame with "Last Detected" and "First Detected" columns
            per group, underwriting date, member and drug for Smart Specialty table
        """
    grouping_features = ['group_id', 'mbr_pers_gen_key', 'underwriting_date', 'milliman_match_name']

    # last and first periods calculation on rx claims (rx_month and underwriting_date)
    qtr_rxdeb_grpd = qtr_rxdeb.groupby(grouping_features)['rx_month'].agg(
        [('min_rx_date', 'min'), ('max_rx_date', 'max')]).reset_index()
    qtr_rxdeb_grpd["when_first_detected"] = get_calendar_month_diff(
        qtr_rxdeb_grpd['min_rx_date'], qtr_rxdeb_grpd['underwriting_date']).apply(map_months_to_range)
    qtr_rxdeb_grpd["when_last_detected"] = get_calendar_month_diff(
        qtr_rxdeb_grpd['max_rx_date'], qtr_rxdeb_grpd['underwriting_date']).apply(map_months_to_range)

    # last and first periods calculation on dx claims (fromdate and underwriting_date)
    qtr_meddeb['fromdate'] = pd.to_datetime(qtr_meddeb['fromdate'])
    qtr_meddeb['underwriting_date'] = pd.to_datetime(qtr_meddeb['underwriting_date'])
    qtr_meddeb_grpd = qtr_meddeb.groupby(grouping_features)['fromdate'].agg(
        [('min_frm_date', 'min'), ('max_frm_date', 'max')]).reset_index()
    qtr_meddeb_grpd["when_first_detected"] = get_calendar_month_diff(
        qtr_meddeb_grpd['min_frm_date'], qtr_meddeb_grpd['underwriting_date']).apply(map_months_to_range)
    qtr_meddeb_grpd["when_last_detected"] = get_calendar_month_diff(
        qtr_meddeb_grpd['max_frm_date'], qtr_meddeb_grpd['underwriting_date']).apply(map_months_to_range)

    output_columns = ["group_id", "mbr_pers_gen_key", "underwriting_date", "milliman_match_name",
                      "when_first_detected", "when_last_detected"]

    # append rx and dx claims
    smart_specialty_periods = qtr_rxdeb_grpd[output_columns].append(qtr_meddeb_grpd[output_columns]).reset_index(
        drop=True)

    return smart_specialty_periods


def get_smart_specialty_table(iqsubmit, cust, cust_pred, qtr_rxdeb,
                              qtr_meddeb, hrdl_debits_clean):
    """Creates Smart Specialty table

    Args:
        iqsubmit: census file for a group
        cust: intermediate DataFrame from production pipeline;
              contains group ids, total members per group, rx debits, med debits per group
        cust_pred: intermediate DataFrame from production pipeline;
                   contains group ids, pfm calibrated rate, predicted risks per group,
                   including rx_debit_rs, pred_risk as final risk per group
        qtr_rxdeb: intermediate DataFrame from production pipeline;
                   contains group and member ids, rx claims data (in times = 1,2,3),
                   enriched with agnostic plan year assumption values and
                   earliest period, latest period values from ndc mapping table
        qtr_meddeb: intermediate DataFrame from production pipeline;
                    contains group and member ids, med (dx) claims data (in times = 1,2,3),
                    enriched with agnostic plan year assumption values and
                    earliest period, latest period values from ndc mapping table
        smart_specialty_periods: DataFrame with last and first detected periods
                                 per group, member, underwriting date and drug;
                                 output of smart_specialty_periods
        hrdl_debits_clean: intermediate pandas DataFrame, original lookup prod_table
    Returns:
        pandas DataFrame with drug claims factors
    """
    smart_specialty_periods = get_smart_specialty_periods(qtr_rxdeb, qtr_meddeb)
    
    # merge rx and med claims together
    qtr_rx_med = qtr_rxdeb.append(qtr_meddeb)
    qtr_rx_med_agg = qtr_rx_med.groupby(['group_id', 'mbr_pers_gen_key', 'milliman_match_name',
                                         'underwriting_date', 'times', 'earliest_period', 'latest_period']
                                        ).agg({'plan_yr_dollar_assumption': max}
                                              ).reset_index()

    # calculate rx debits risk score ("Impact to Group Score") per member per drug
    # only for claims from 0-3 month time period (times <= earliest period & times>= latest period)
    rx_debits = qtr_rx_med_agg[(qtr_rx_med_agg.times <= qtr_rx_med_agg.earliest_period) &
                               (qtr_rx_med_agg.times >= qtr_rx_med_agg.latest_period)]
    cust = cust[['group_id', 'underwriting_date', 'total_members']]
    rx_debits_cust = pd.merge(rx_debits, cust, how='inner', on=['group_id', 'underwriting_date'])
    cust_pred = cust_pred[['src_rpt_cust_id', 'underwriting_date', 'pfm_calibrated']]
    rx_debits_cust = pd.merge(rx_debits_cust, cust_pred, how='inner', left_on=['group_id', 'underwriting_date'],
                              right_on=['src_rpt_cust_id', 'underwriting_date'])
    rx_debits_cust['impact_to_grprs'] = rx_debits_cust['plan_yr_dollar_assumption'] / (12 * rx_debits_cust['total_members']
                                                       * rx_debits_cust['pfm_calibrated'])
    rx_debits_cust['impact_to_grprs'] = rx_debits_cust['impact_to_grprs'].astype(float) \
        .round(ROUND_DECIMALS)

    # merge impact to group score back to claims data
    rx_debits_cust = rx_debits_cust[['group_id', 'mbr_pers_gen_key', 'milliman_match_name',
                                     'underwriting_date', 'times', 'impact_to_grprs']]
    qtr_rx_med_final = pd.merge(qtr_rx_med_agg, rx_debits_cust, how='left',
                                on=['group_id', 'mbr_pers_gen_key', 'milliman_match_name',
                                    'underwriting_date', 'times'])

    # take maximum impact per group, member, drug
    smart_specialty_table = qtr_rx_med_final.groupby(['group_id', 'mbr_pers_gen_key', 'milliman_match_name',
                                                      'underwriting_date', 'plan_yr_dollar_assumption']
                                                     ).agg({'impact_to_grprs': max}).reset_index()

    # set impact as "Unknown" for rows, not accounted in impact calculation
    #smart_specialty_table['impact_to_grprs'] = smart_specialty_table['impact_to_grprs'].fillna('Unknown')

    # merge last and first detected time periods
    smart_specialty_table = pd.merge(smart_specialty_table, smart_specialty_periods,
                                     how='inner', on=["group_id", "mbr_pers_gen_key", "underwriting_date",
                                                      "milliman_match_name"])

    #
    male_adults, female_adults, children = get_age_sex_stats(iqsubmit)
    prod_filtered = filter_prod_info(hrdl_debits_clean, male_adults, female_adults, children)
    smart_specialty_table = pd.merge(smart_specialty_table, prod_filtered,
                                     how='inner', on='milliman_match_name')

    # sort DataFrame by member cost descending order
    smart_specialty_table['when_last_detected'] = pd.Categorical(smart_specialty_table['when_last_detected'], 
                      categories=["13+ Months to UW","4-12 Months to UW","0-3 Months to UW"],
                      ordered=True)
    smart_specialty_table = smart_specialty_table.sort_values(
        ['when_last_detected','plan_yr_dollar_assumption'],
        ascending=False)

    # select required fields under business names
    smart_specialty_table.rename(columns={
        'prod grouping': 'Drug Name',
        'plan_yr_dollar_assumption': 'Assumed Annual Cost of Use',
        'impact_to_grprs': 'Estimated Impact to Group Score',
        'when_first_detected': 'When First Detected',
        'when_last_detected': 'When Last Detected'}, inplace=True)
    smart_specialty_table = smart_specialty_table[['Drug Name', 
                                                   'When Last Detected', 
                                                   'Assumed Annual Cost of Use', 
                                                   'Estimated Impact to Group Score' ]] \
        .reset_index(drop=True)
    filtered_smart_specialty_table = filter_smart_specialty_table(smart_specialty_table)
    return filtered_smart_specialty_table

def filter_smart_specialty_table(smart_specialty_table):
    smart_specialty_table['Estimated Impact to Group Score']=smart_specialty_table["Estimated Impact to Group Score"].convert_objects(convert_numeric=True)
    filtered_smart_specialty_table=smart_specialty_table.loc[(smart_specialty_table['Assumed Annual Cost of Use'] >= 1000)| (smart_specialty_table['Estimated Impact to Group Score']>= 0.01)]
    filtered_smart_specialty_table['Estimated Impact to Group Score'] = filtered_smart_specialty_table['Estimated Impact to Group Score'].fillna("Lower/Not Detected Recently")
    
    return filtered_smart_specialty_table

"""Diagnosis (high-cost conditions) table construction"""
from abc import ABC, abstractmethod
from enum import Enum
from itertools import chain
import numpy as np
import pandas as pd
import shap

MIN_QUARTER = 1
MAX_QUARTER = 3
BASE_SHAP_COLUMN = "base_shap"
EPS = 0.01

def get_smart_diagnosis_table(iqsubmit, medvertdta, qtr_meddta,
                              iq_cptvert, iq_revvert, icd09lookup, icd10lookup, debjoin3,
                              features_for_explanation, predictions_all, cust_pred):
    
    """Create a high-cost conditions table

    Args:
        diag_table_no_impact: pandas DataFrame constructed by
            get_diagnosis_table
        hcup_impacts: pandas DataFrame constructed by
            get_hcup_impacts
        cust_pred: pandas DataFrame from the production code;
            contains a single row with a group info
    Returns:
        pandas DataFrame
    """
    
    hcup_prev = read_hcup_prev_table(os.path.join(DATA_PATH, "hcup_prevalance.csv"))

    
    diag_table_no_impact = get_diagnosis_table(iqsubmit, medvertdta, qtr_meddta, hcup_prev, icd09lookup, icd10lookup)
    
    models = {MemberBasicModelName.INPATIENT_Q1: gbm_Q1_ixx,
          MemberBasicModelName.INPATIENT_Q2: gbm_Q2_ixx,
          MemberBasicModelName.INPATIENT_Q3: gbm_Q3_ixx,
          MemberBasicModelName.OUTPATIENT_Q1: gbm_Q1_xox,
          MemberBasicModelName.OUTPATIENT_Q2: gbm_Q2_xox,
          MemberBasicModelName.OUTPATIENT_Q3: gbm_Q3_xox,
          MemberStackedModelName.MED: xgb_med, 
          MemberStackedModelName.OP: xgb_op,
          MemberStackedModelName.OP_RX: xgb_op_rx,
          MemberStackedModelName.IOR: xgb_ior,
          GroupModelName.GROUP: xgb_grp}
    
    
    
    hcup_costs = get_hcup_associated_costs(iq_cptvert, iq_revvert, icd09lookup, icd10lookup, debjoin3)
    hcup_impacts = get_hcup_impacts(features_for_explanation, predictions_all, models, hcup_costs=hcup_costs)
    
    
    conditions_table = diag_table_no_impact.merge(hcup_impacts,
                                                  left_on=['mbr_pers_gen_key',
                                                           'group_id', 'hcup'],
                                                  right_on=['mbr_pers_gen_key',
                                                            'src_rpt_cust_id', 'hcup'])
    conditions_table = conditions_table.merge(cust_pred,
                                              left_on=['group_id'],
                                              right_on=['src_rpt_cust_id'])
    conditions_table['score_impact'] = conditions_table['impact'] / \
                                       conditions_table['pfm_calibrated']
    conditions_table = conditions_table[['description',
                                         'mode_icd',
                                         'yr',
                                         'impact',
                                         'score_impact',
                                         'base_value',
                                         'last_detected_range',
                                         'first_detected_range']]
    conditions_table['score_impact'] = conditions_table['score_impact'].astype(float) \
        .round(ROUND_DECIMALS)
    conditions_table['impact'] = conditions_table['impact'].astype(float) \
        .round(ROUND_DECIMALS)
    conditions_table.sort_values('yr', ascending=False, inplace=True)
    conditions_table = conditions_table.rename(columns={'impact': 'Group Cost Impact',
                         'score_impact': 'Approximate Impact on Group Level Score',
                         'description': 'Diagnosis Grouping',
                         'yr': 'Member Cost',
                         'base_value': 'Base Group Cost Prediction',
                         'last_detected_range': 'When Last Detected',
                         'first_detected_range': 'When First Detected'}).dropna(axis=1)
    filtered_smart_diagnosis_table = trans_smart_diagnosis_table(conditions_table)
        
    return filtered_smart_diagnosis_table

def trans_smart_diagnosis_table(smart_diagnosis_table):
    
    icds = pd.read_csv('/home/nhp1343/elliott-feb_2/MCC_ICD10_HCUP_MAPPING.csv')
    
    male_adults, female_adults, children = get_age_sex_stats(iqsubmit)
    #icds['display_flag'][icds.adult_male<male_adults]='N'
    
    smart_diagnosis_table.sort_values(['Approximate Impact on Group Level Score'], ascending=False, inplace=True)
    smart_diagnosis_table = smart_diagnosis_table.reset_index()
    filtered_smart_diagnosis_table = smart_diagnosis_table[(smart_diagnosis_table['Member Cost']>=1000) | (smart_diagnosis_table['Approximate Impact on Group Level Score']>= 0.01)]
    filtered_smart_diagnosis_table['Impact Rank'] = filtered_smart_diagnosis_table.index + 1
    filtered_smart_diagnosis_table=filtered_smart_diagnosis_table[['Diagnosis Grouping', 'mode_icd', 'When Last Detected','When First Detected','Impact Rank']]
    filtered_smart_diagnosis_table = filtered_smart_diagnosis_table.merge(icds[['diag', 'DIAG_DESC']], left_on = 'mode_icd', right_on = 'diag', how = 'left').drop('diag', axis = 1)
    filtered_smart_diagnosis_table['DIAG_DESC'] = filtered_smart_diagnosis_table['DIAG_DESC'].str.title()
    filtered_smart_diagnosis_table.columns=['Diagnosis Group (HCUP) Name','Most Frequent ICD10 Code', 'When Last Detected','When First Detected','Impact Rank', 'Most Frequent ICD10 Description']
    
    filtered_smart_diagnosis_table = filtered_smart_diagnosis_table.merge(icds[['diag', 'adult_male', 'adult_female', 'children','display_flag']],
                                                                          left_on = 'Most Frequent ICD10 Code', right_on='diag', 
                                                                          how = 'inner')
    
    filtered_smart_diagnosis_table['Most Frequent ICD10 Code'][filtered_smart_diagnosis_table['adult_male']>male_adults] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Code'][filtered_smart_diagnosis_table['adult_female']>female_adults] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Code'][filtered_smart_diagnosis_table['children']>children] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Code'][filtered_smart_diagnosis_table['display_flag']=="N"] = ''
    
    filtered_smart_diagnosis_table['Most Frequent ICD10 Description'][filtered_smart_diagnosis_table['adult_male']>male_adults] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Description'][filtered_smart_diagnosis_table['adult_female']>female_adults] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Description'][filtered_smart_diagnosis_table['children']>children] = ''
    filtered_smart_diagnosis_table['Most Frequent ICD10 Description'][filtered_smart_diagnosis_table['display_flag']=="N"] = ''
    
    return filtered_smart_diagnosis_table[['Diagnosis Group (HCUP) Name','Most Frequent ICD10 Code', 'Most Frequent ICD10 Description', 'When Last Detected','When First Detected','Impact Rank']]



def get_hcup_associated_costs(iq_cptvert, iq_revvert, icd09lookup, icd10lookup, debjoin3):
    """Get costs associated with hcups (used for feature engineering)"""
    diags = [col for col in iq_cptvert if col.startswith('diag')]
    grouping_columns = ['group_id', 'src_rpt_cust_id',
                        'mbr_pers_gen_key', 'underwriting_date',
                        'times', 'med_sample']
    iq_cptvert_ext = iq_cptvert.merge(debjoin3, on=grouping_columns, how='left')
    iq_cptvert_ext['allowed'] = iq_cptvert_ext['ndc_allowed'] \
        .fillna(iq_cptvert_ext['cpt_allowed']) \
        .fillna(iq_cptvert_ext['charged']).fillna(0)
    iq_cptvert_ext['allowed'] = np.where(iq_cptvert_ext['allowed'] >
                                         iq_cptvert_ext['charged'],
                                         iq_cptvert_ext['charged'],
                                         iq_cptvert_ext['allowed'])
    iq_cptvert_ext['cpt_allowed'] = iq_cptvert_ext['debcost'].fillna(iq_cptvert_ext['allowed'])
    iq_cptvert_ext.drop(columns=['allowed'], inplace=True)
    cpt_diag_costs = iq_cptvert_ext.melt(id_vars=grouping_columns + ['cpt_allowed'],
                                         value_vars=diags,
                                         value_name='value').dropna(subset=['value']) \
        .drop(columns='variable')
    rev_diag_costs = iq_revvert.melt(id_vars=grouping_columns + ['rev_allowed'],
                                     value_vars=diags,
                                     value_name='value').dropna(subset=['value']) \
        .drop(columns='variable')
    cpt_hcup_costs = pd.concat([cpt_diag_costs.merge(icd,
                                                     left_on=['value'],
                                                     right_on=['diag_cd'])
                                [grouping_columns + ['hcup', 'cpt_allowed']] \
                                for icd in [icd09lookup, icd10lookup]], 0)
    rev_hcup_costs = pd.concat([rev_diag_costs.merge(icd,
                                                     left_on=['value'],
                                                     right_on=['diag_cd'])
                                [grouping_columns + ['hcup', 'rev_allowed']] \
                                for icd in [icd09lookup, icd10lookup]], 0)
    cpt_hcup_costs['underwriting_date'] = pd.to_datetime(cpt_hcup_costs['underwriting_date'])
    rev_hcup_costs['underwriting_date'] = pd.to_datetime(rev_hcup_costs['underwriting_date'])
    hcup_costs = cpt_hcup_costs.groupby(grouping_columns + ['hcup'], as_index=False).agg({'cpt_allowed': sum}) \
        .merge(rev_hcup_costs.groupby(grouping_columns + ['hcup'], as_index=False).agg({'rev_allowed': sum}),
               on=grouping_columns + ['hcup'], how='outer').fillna(0)
    hcup_costs['combined_allowed'] = hcup_costs['cpt_allowed'] + hcup_costs['rev_allowed']
    for feature in ['cpt_allowed', 'rev_allowed', 'combined_allowed']:
        hcup_costs[f'weight_{feature}'] = hcup_costs[feature] / \
                                          (hcup_costs.groupby(grouping_columns)[feature]
                                           .transform(sum) + EPS)
    return hcup_costs


def filter_hcup_info(hcup_prev, male_adults, female_adults, children):
    """Filter hcup table"""
    is_hidden = (hcup_prev['display_flag'] == 'N') | \
                (hcup_prev['adult_male'] > male_adults) | \
                (hcup_prev['adult_female'] > female_adults) | \
                (hcup_prev['children'] > children)
    hcup_prev['description'][is_hidden] = HIDDEN_PLACEHOLDER
    hcup_prev['yr'][is_hidden] = np.nan


def _melt_diagnosis_codes(qtr_meddta):
    """Melt diagnosis codes in med data"""
    id_vars = ['group_id', 'mbr_pers_gen_key', 'underwriting_date',
               'mth_from_uwdate', 'times', 'fromdate']
    diags = [col for col in qtr_meddta if col.startswith('diag')]
    diagvert_extended = qtr_meddta.melt(id_vars=id_vars,
                                        value_vars=diags,
                                        value_name='value').dropna(subset=['value']) \
        .drop(columns='variable')
    return diagvert_extended


def get_diagnosis_table(iqsubmit, medvertdta, qtr_meddta, hcup_prev, icd09lookup, icd10lookup):
    """Create diagnosis table

    Args:
        iqsubmit: census DataFrame for a group
        medvertdta: intermediate DataFrame from production pipeline;
            contains group ids, member keys, hcup values, etc.
        qtr_meddta: intermediate DataFrame from production pipeline;
            contains group ids, member keys, diagnosis codes,
            underwriting dates, fromdate, etc.
        hcup_prev: hcup prevalence DataFrame
        icd09lookup: icd09 lookup DataFrame
        icd10lookup: icd10 lookup DataFrame
    Returns:
        pandas DataFrame containing diagnosis info
    """
    male_adults, female_adults, children = get_age_sex_stats(iqsubmit)
    filter_hcup_info(hcup_prev, male_adults, female_adults, children)

    medunique = medvertdta[medvertdta['datatype'] == 'md'] \
            [['group_id', 'mbr_pers_gen_key', 'value']].drop_duplicates()
    med_info = medunique.merge(hcup_prev, on='value', how='inner')
    selected_columns = ['group_id', 'mbr_pers_gen_key', 'underwriting_date',
                            'hcup', 'diag_cd', 'mth_from_uwdate', 'fromdate']
    diagvert = _melt_diagnosis_codes(qtr_meddta)
    diagnosis_dates = pd.concat([diagvert.merge(icd,
                                                    left_on=['value'],
                                                    right_on=['diag_cd'])[selected_columns] \
                                     for icd in [icd09lookup, icd10lookup]], 0)
    grouping_columns = ['group_id', 'mbr_pers_gen_key', 'underwriting_date', 'hcup']
    diagnosis_dates['mth_from_uw'] = get_calendar_month_diff(diagnosis_dates['fromdate'],
                                                                 diagnosis_dates['underwriting_date'])
    diagnosis_dates['last_detected'] = diagnosis_dates.groupby(grouping_columns)['mth_from_uw'] \
            .transform(min)
    diagnosis_dates['first_detected'] = diagnosis_dates.groupby(grouping_columns)['mth_from_uw'] \
            .transform(max)
    diagnosis_dates['counts'] = diagnosis_dates.groupby(['mbr_pers_gen_key', 'hcup', 'diag_cd'])['fromdate'].transform('count')
    diagnosis_dates = diagnosis_dates.reset_index()
    diagnosis_dates['mode'] = diagnosis_dates.groupby(['mbr_pers_gen_key', 'hcup'], sort=False)['counts'].transform(pd.Series.idxmax)

    diagnosis_dates['mode_icd'] = diagnosis_dates[diagnosis_dates.index==diagnosis_dates['mode']]['diag_cd']
    modes = diagnosis_dates[['mbr_pers_gen_key', 'hcup','mode_icd']].dropna()#diagnosis_dates=diagnosis_dates.dropna()
    diagnosis_dates = diagnosis_dates.drop(columns = ['mode','mode_icd','counts'])
    diagnosis_dates = diagnosis_dates.merge(modes, on = ['mbr_pers_gen_key', 'hcup'])
    diagnosis_table = diagnosis_dates[['group_id', 'mbr_pers_gen_key',
                                           'underwriting_date', 'hcup', 'mode_icd', 'last_detected',
                                           'first_detected']].drop_duplicates()
    diagnosis_table = diagnosis_table.merge(med_info,
                                                left_on=['group_id', 'mbr_pers_gen_key', 'hcup'],
                                                right_on=['group_id', 'mbr_pers_gen_key', 'value'],
                                                how='inner')
    diagnosis_table = diagnosis_table[['group_id', 'mbr_pers_gen_key',
                                           'description', 'yr', 'hcup', 'mode_icd',
                                           'last_detected', 'first_detected']]
    for months_column in ['first_detected', 'last_detected']:
            diagnosis_table[months_column + '_range'] = diagnosis_table[months_column] \
                .apply(map_months_to_range)
    return diagnosis_table


def get_hcup_impacts(features_for_explanation, predictions_all, models, hcup_costs=None):
    """Calculate hcup impacts in group level score

    Args:
        features_for_explanation: dict with feature DataFrames
            from the production code
        predictions_all: pandas DataFrame with member-level predictions
        models: dict with member-level and group-level models
        hcup_costs: pandas DataFrame constructed by get_hcup_associated_costs
    Returns:
        pandas DataFrame containing member-level hcup impacts
            on group score
    """
    feature_contributions = {}
    member_model_explainer = MemberBasicModelExplainer(features_for_explanation)
    for model_name in MemberBasicModelName:
        model = models[model_name]
        feature_contributions[model_name] = member_model_explainer \
            .get_hcup_shap_values(model, model_name, hcup_costs)

    member_stacked_model_explainer = MemberStackedModelExplainer(predictions_all)
    for model_name in MemberStackedModelName:
        model = models[model_name]
        feature_contributions[model_name] = member_stacked_model_explainer \
            .get_hcup_shap_values(model, model_name)

    group_model_explainer = GroupModelExplainer(features_for_explanation['group'])
    group_contrib = group_model_explainer.get_shap_values(models[GroupModelName.GROUP])
    group_feature_contrib = {'score': group_contrib['log_score_shap'] +
                                      group_contrib['bc_score_shap'] +
                                      group_contrib['score_shap'] +
                                      group_contrib['score_ttl_shap'] +
                                      group_contrib['log_score_pct_shap'] +
                                      group_contrib['bc_score_pct_shap']}
    for model_type in chain(MemberBasicModelName, MemberStackedModelName):
        group_feature_contrib[model_type] = group_contrib[f'pred_{model_type.value}_mean_shap'] + \
                                            group_contrib[f'pred_{model_type.value}_pct_shap']

    op_shap, op_rx_shap, med_shap, ior_shap = get_member_models_contrib(group_feature_contrib,
                                                                        feature_contributions)

    key_columns = ['mbr_pers_gen_key', 'underwriting_date', 'src_rpt_cust_id']
    for model_shap in [op_rx_shap, op_shap, med_shap, ior_shap]:
        model_shap.set_index(key_columns, inplace=True)

    predictions_indexed = predictions_all.set_index(key_columns, inplace=False)

    for model_name in MemberStackedModelName:
        feature_contributions[model_name].set_index(key_columns, inplace=True)
    basic_shap = get_basic_pred_shap(group_feature_contrib,
                                     ior_shap, med_shap, op_rx_shap, op_shap,
                                     feature_contributions,
                                     predictions_indexed)

    hcup_contrib = get_hcup_contrib_from_basic_pred(basic_shap, feature_contributions, key_columns)

    contributions = hcup_contrib.reset_index() \
        .melt(id_vars=key_columns, value_vars=[c for c in hcup_contrib if c.endswith("_shap")],
              var_name="hcup_shap", value_name="impact")
    contributions['code'] = contributions['hcup_shap'].apply(lambda s: s.split('_')[1])

    features = hcup_contrib.reset_index(). \
        melt(id_vars=key_columns, value_vars=[c for c in hcup_contrib if (
            c.startswith("hcup") and not c.endswith("_shap"))],
             var_name="hcup", value_name="is_feature")
    features['code'] = features['hcup'].apply(lambda s: s.split('_')[1])

    hcup_impacts = contributions.merge(features, on=key_columns + ['code'])
    hcup_impacts = hcup_impacts[hcup_impacts.is_feature > 0]
    hcup_impacts.drop(columns=['is_feature'], inplace=True)
    hcup_impacts['base_value'] = group_model_explainer.base_value
    return hcup_impacts


def get_hcup_contrib_from_basic_pred(basic_shap, feature_contributions, key_columns):
    """Utility function for hcup impacts calculation"""
    hcup_contrib = None
    for i, model_type in enumerate(MemberBasicModelName):
        features_shap = basic_shap.merge(feature_contributions[model_type],
                                         on=key_columns, how='left')
        features_shap.set_index(key_columns, inplace=True)
        hcup_shap_columns = [c for c in features_shap if c.endswith("_shap")]
        features_shap[hcup_shap_columns] = features_shap[hcup_shap_columns].multiply(
            features_shap[f'pred_{model_type.value}_shap'] / features_shap['shap_sum'],
            axis='index')
        current_shap = features_shap[[c for c in features_shap if c.startswith("hcup_")]]
        if i == 0:
            hcup_contrib = current_shap.fillna(0)
        else:
            hcup_contrib = current_shap.fillna(0) + hcup_contrib
            hcup_contrib.fillna(0, inplace=True)
    return hcup_contrib


def get_member_models_contrib(group_feature_contrib, feature_contributions):
    """Utility function for hcup impacts calculation"""

    def _get_contrib(group_feature_contrib, feature_contributions, model_key):
        values_shap = feature_contributions[model_key][[c for c in feature_contributions[model_key]
                                                        if c.endswith("_shap")]].copy()
        values_sum = values_shap.sum().sum()
        values_shap *= group_feature_contrib[model_key] / values_sum
        values_shap.drop(columns=[BASE_SHAP_COLUMN], inplace=True)
        return pd.concat([values_shap,
                          feature_contributions[model_key]
                          [['mbr_pers_gen_key', 'underwriting_date', 'src_rpt_cust_id']]], 1)

    op_shap = _get_contrib(group_feature_contrib, feature_contributions,
                           MemberStackedModelName.OP)
    for i in range(MIN_QUARTER, MAX_QUARTER + 1):
        op_shap[f'pred_xxr_{i}_shap'] = 0
        op_shap[f'pred_ixx_{i}_shap'] = 0
    op_rx_shap = _get_contrib(group_feature_contrib, feature_contributions,
                              MemberStackedModelName.OP_RX)
    for i in range(MIN_QUARTER, MAX_QUARTER + 1):
        op_rx_shap[f'pred_ixx_{i}_shap'] = 0
    med_shap = _get_contrib(group_feature_contrib, feature_contributions,
                            MemberStackedModelName.MED)
    for i in range(MIN_QUARTER, MAX_QUARTER + 1):
        med_shap[f'pred_xxr_{i}_shap'] = 0
    ior_shap = _get_contrib(group_feature_contrib, feature_contributions,
                            MemberStackedModelName.IOR)
    return op_shap, op_rx_shap, med_shap, ior_shap


def get_basic_pred_shap(group_feature_contrib, ior_shap, med_shap, op_rx_shap, op_shap,
                        feature_contributions, predictions_all):
    """Utility function for hcup impacts calculation"""
    pred_columns = [f'pred_ixx_{i}_shap' for i in range(MIN_QUARTER, MAX_QUARTER + 1)] + \
                   [f'pred_xox_{i}_shap' for i in range(MIN_QUARTER, MAX_QUARTER + 1)] + \
                   [f'pred_xxr_{i}_shap' for i in range(MIN_QUARTER, MAX_QUARTER + 1)] + \
                   ['base_shap']
    for column in pred_columns:
        for model_name in MemberStackedModelName:
            if model_name not in feature_contributions:
                continue
            if column not in feature_contributions[model_name]:
                feature_contributions[model_name][column] = 0
    score_op_rx = feature_contributions[MemberStackedModelName.OP_RX][pred_columns] \
        .multiply(predictions_all['ind_op_rx'], axis='index')
    score_ior = feature_contributions[MemberStackedModelName.IOR][pred_columns] \
        .multiply(predictions_all['ind_ior'], axis='index')
    score_op = feature_contributions[MemberStackedModelName.OP][pred_columns] \
        .multiply(predictions_all['ind_op'], axis='index')
    score_med = feature_contributions[MemberStackedModelName.MED][pred_columns] \
        .multiply(predictions_all['ind_med'], axis='index')
    score_shap = score_op_rx + score_ior + score_op + score_med
    score_shap *= group_feature_contrib['score'] / score_shap.sum().sum()
    score_shap.drop(columns=[BASE_SHAP_COLUMN], inplace=True)
    for model_name in MemberBasicModelName:
        weights = (predictions_all[f'pred_{model_name.value}'] + EPS) / \
                  (predictions_all[f'pred_{model_name.value}'].sum() + EPS)
        score_shap[f'pred_{model_name.value}_shap'] += group_feature_contrib[model_name] * weights

    return score_shap + op_shap + op_rx_shap + med_shap + ior_shap


class MemberBasicModelName(Enum):
    """Basic member model names"""
    INPATIENT_Q1 = 'ixx_1'
    INPATIENT_Q2 = 'ixx_2'
    INPATIENT_Q3 = 'ixx_3'
    OUTPATIENT_Q1 = 'xox_1'
    OUTPATIENT_Q2 = 'xox_2'
    OUTPATIENT_Q3 = 'xox_3'
    # RX_Q1 = 'xxr_1'
    # RX_Q2 = 'xxr_2'
    # RX_Q3 = 'xxr_3'


class MemberStackedModelName(Enum):
    """Stacked member model names"""
    MED = 'med'
    OP = 'op'
    IOR = 'ior'
    OP_RX = 'op_rx'
    # RX = 'rx'


class GroupModelName(Enum):
    """Group model names"""
    GROUP = 'group'


class MemberModelExplainer(ABC):
    """Abstract class for hcup SHAP calculation"""

    def __init__(self, features):
        self.features = features

    def load_features(self, model_name):
        """Features are taken by model name"""
        features = self.features[model_name.value]
        member_key = self.features[f'{model_name.value}_mbr_key']
        return features, member_key

    @abstractmethod
    def get_hcup_shap_values(self, model, model_name, hcup_costs=None):
        """HCUP SHAP values calculation"""
        pass


class MemberBasicModelExplainer(MemberModelExplainer):
    """HCUP SHAP calculation for basic member-level models"""

    def get_hcup_shap_values(self, model, model_name, hcup_costs=None):
        """Calculate SHAP values"""
        features, member_key, shap_values = self._explain(model, model_name)
        cost_features = {'ag_allow_ip_pmpm', 'ag_allow_op_pmpm',
                         'cpt_ag_allow_amt', 'rev_ag_allow_amt'}
        hcup_feature_map = {key: i for i, key in enumerate(features.columns)
                            if key.startswith("hcup_")
                            or ((hcup_costs is not None) and (key in cost_features))}
        shap_df = pd.DataFrame(shap_values[:, [hcup_feature_map[key] for key in hcup_feature_map]],
                               columns=[key + '_shap' for key in hcup_feature_map])

        features_shap = pd.concat([member_key.reset_index(), shap_df,
                                   features[hcup_feature_map.keys()].reset_index()],
                                  1).drop(['index'], 1)
        columns = features_shap.columns
        if hcup_costs is not None:
            features_shap = self._include_costs_shap(features_shap,
                                                     hcup_costs, model_name)
            features_shap = features_shap[columns]
        features_shap['shap_sum'] = shap_values.sum(1)
        return features_shap

    def _include_costs_shap(self, features_shap, hcup_costs, model_name):
        name_parts = model_name.value.split('_')
        quarter = int(name_parts[-1])
        model_type = name_parts[0]
        med_type = "i" if model_type == "ixx" else "o"
        hcup_costs_impacts, features_shap = self._get_hcup_cost_impacts(hcup_costs, features_shap,
                                                                        med_type, quarter)
        columns = hcup_costs_impacts.columns
        features_shap[columns] = features_shap[columns].fillna(0) + \
                                 hcup_costs_impacts[columns].fillna(0)
        return features_shap.reset_index()

    def _get_hcup_cost_impacts(self, hcup_costs, model_feature_contributions,
                               med_type='o', quarter=3):
        """Heuristic calculation of hcup additional impact based on the
        associated cpt and rev costs"""

        def _multiindex_pivot(df, columns=None, values=None):
            # https://github.com/pandas-dev/pandas/issues/23955
            names = list(df.index.names)
            df = df.reset_index()
            list_index = df[names].values
            tuples_index = [tuple(i) for i in list_index]  # hashable
            df = df.assign(tuples_index=tuples_index)
            df = df.pivot(index="tuples_index", columns=columns, values=values)
            tuples_index = df.index  # reduced
            index = pd.MultiIndex.from_tuples(tuples_index, names=names)
            df.index = index
            return df

        costs = hcup_costs[(hcup_costs.med_sample == med_type) & (hcup_costs.times == quarter)]
        index = ['underwriting_date', 'src_rpt_cust_id', 'mbr_pers_gen_key']
        costs.set_index(index, inplace=True)
        costs.fillna(0, inplace=True)
        costs['hcup'] = costs['hcup'].apply(lambda s: s + "_shap")
        additional_hcup_impacts = None
        for weight_column, feature in zip(['weight_cpt_allowed',
                                           'weight_rev_allowed',
                                           'weight_combined_allowed'],
                                          ['cpt_ag_allow_amt_shap',
                                           'rev_ag_allow_amt_shap',
                                           f'ag_allow_{med_type}p_pmpm_shap']):
            if feature not in model_feature_contributions:
                continue
            weights_pivot = _multiindex_pivot(costs, columns='hcup',
                                              values=[weight_column]).fillna(0)
            weights_pivot.columns = weights_pivot.columns.droplevel()
            weights_pivot.reset_index(inplace=True)
            hcup_cost_impacts = weights_pivot.merge(model_feature_contributions[index + [feature]])
            for column in hcup_cost_impacts:
                if column.startswith("hcup_"):
                    hcup_cost_impacts[column] *= hcup_cost_impacts[feature]
            hcup_cost_impacts.set_index(index, inplace=True)
            if additional_hcup_impacts is None:
                additional_hcup_impacts = hcup_cost_impacts.fillna(0)
            else:
                additional_hcup_impacts = additional_hcup_impacts + hcup_cost_impacts.fillna(0)
                additional_hcup_impacts.fillna(0, inplace=True)
        model_feature_contributions.set_index(index, inplace=True)
        return additional_hcup_impacts, model_feature_contributions

    def _explain(self, model, model_name):
        features, member_key = self.load_features(model_name)
        explainer = shap.TreeExplainer(model)
        if features.size > 0:
            shap_values = explainer.shap_values(features.values)
        else:
            shap_values = np.zeros((member_key.shape[0], features.shape[1]))
        return features, member_key, shap_values


class MemberStackedModelExplainer(MemberModelExplainer):
    """HCUP SHAP calculation for stacked member-level models"""

    def __init__(self, features):
        super().__init__(features)
        self.model_features = {MemberStackedModelName.MED:
                                   ['pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3',
                                    'pred_xox_1', 'pred_xox_2', 'pred_xox_3'],
                               MemberStackedModelName.OP:
                                   ['pred_xox_1', 'pred_xox_2', 'pred_xox_3'],
                               MemberStackedModelName.OP_RX:
                                   ['pred_xox_1', 'pred_xox_2', 'pred_xox_3',
                                    'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3'],
                               MemberStackedModelName.IOR:
                                   ['pred_ixx_1', 'pred_ixx_2', 'pred_ixx_3',
                                    'pred_xox_1', 'pred_xox_2', 'pred_xox_3',
                                    'pred_xxr_1', 'pred_xxr_2', 'pred_xxr_3'],
                               }
        self.member_key = ['mbr_pers_gen_key', 'underwriting_date', 'src_rpt_cust_id']

    def get_hcup_shap_values(self, model, model_name, hcup_costs=None):
        """Calculate SHAP values"""
        if model_name not in self.model_features:
            raise ValueError("Not implemented.")
        explainer = shap.TreeExplainer(model)
        features = self.features[self.model_features[model_name]]
        if features.values.size > 0:
            shap_values = explainer.shap_values(features.values)
        else:
            shap_values = np.zeros((self.features[self.member_key].shape[0],
                                    features.shape[1]))
        shap_df = pd.DataFrame(shap_values,
                               columns=[f + '_shap' for f in self.model_features[model_name]])
        features_shap = pd.concat([self.features[self.member_key],
                                   shap_df,
                                   features], 1)
        features_shap[BASE_SHAP_COLUMN] = explainer.expected_value
        return features_shap


class GroupModelExplainer:
    """HCUP SHAP calculation for a group-level model"""

    def __init__(self, features):
        self.features = features
        self.model_features = ['quoted_members', 'quoted_pfm_rel', 'comm_rt', 'lf_group',
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
                               'pred_ior_count', 'pred_op_mean',
                               'pred_op_count', 'pred_op_rx_mean',
                               'pred_op_rx_count', 'pred_rx_mean',
                               'pred_rx_count', 'pred_med_mean',
                               'pred_med_count', 'offset', 'score_pct', 'pred_xox_1_pct',
                               'pred_xox_2_pct', 'pred_xox_3_pct',
                               'pred_xxr_1_pct', 'pred_xxr_2_pct',
                               'pred_xxr_3_pct', 'pred_ixx_1_pct',
                               'pred_ixx_2_pct', 'pred_ixx_3_pct',
                               'pred_ior_pct', 'pred_op_pct',
                               'pred_op_rx_pct', 'pred_rx_pct',
                               'pred_med_pct', 'bc_score', 'log_score',
                               'score_ttl', 'log_score_pct',
                               'bc_score_pct', 'year2016', 'year2017',
                               'year2018', 'new_ind']
        self.base_value = None

    def get_shap_values(self, model):
        """Calculate SHAP values"""
        explainer = shap.TreeExplainer(model)
        self.base_value = explainer.expected_value[0]
        features = self.features[self.model_features]
        shap_values = explainer.shap_values(features.values)
        shap_df = pd.DataFrame(shap_values,
                               columns=[f'{feature}_shap' for feature in self.model_features])
        features_shap = pd.concat([shap_df, features.reset_index()], 1)
        return features_shap.iloc[0]
    
    
#---------------------------------------------------------------------------------------------------
        