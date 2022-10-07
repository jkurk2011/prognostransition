"""Module with constant values used in different processors."""

# limiting values
MIN_HISTORICAL_PERIOD_LENGTH = 0
MAX_HISTORICAL_PERIOD_LENGTH = 24
MIN_COV_MONTH_MEDICAL = 6
PLAN_YEAR_DURATION = 12

# codes for different claim types
RX_CLAIM_CODE = 'r'
INPATIENT_CLAIM_CODE = 'i'
OUTPATIENT_CLAIM_CODE = 'o'

# subscriber_flag
SUBSCRIBER_REL_TYPE = '18'
SUBSCRIBER = 1
DEPENDENT = 0

# member/group level included/excluded
INCLUDED = 1
EXCLUDED = 0

# med claim datatypes
REVENUE_CODE = 'mr'
CPT_CODE = 'mc'
DIAGNOSIS_CODE = 'md'

# min/max possible amounts of diagnosis codes
NUM_MIN_DIAG_COL = 2
NUM_MAX_DIAG_COL = 25

# date limits
MIN_CLAIM_DATE = '2015-10-01'
MAX_CLAIM_DATE = '2020-12-31'
MIN_EFF_DATE = '2018-01-01'
MAX_EFF_DATE = '2020-01-01'

# uncategorized
COMMERCIAL_PLATFORMS = ['EM', 'LV']
DAYS_IN_YEAR_AVG = 365.25
MTH_BETWEEN_UW_AND_EFF_DATES = 3
DUMMY_GENETIC_CODE = '99999'
HCUP_UNKNOWN = 'hcup_unk'
