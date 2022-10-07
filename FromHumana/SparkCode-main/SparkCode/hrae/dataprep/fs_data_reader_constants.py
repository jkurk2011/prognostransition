# Constants defining sources, filenames and directories

# Storage
STORAGE_PREFIX_TMP = 'tempdata'
STORAGE_ACCT_NAME = 'dha0mlp0prod'

# Rx Claims
RX_CLAIMS_PATH = '/lvl1/mth_rxclm_line_fact/'

# Med Claims
MED_CLAIMS_PATH = 'abfss://tempdata@dha0mlp0prod.dfs.core.windows.net/lvl1/medclm_mth_line_fact/'
MED_CLAIMS_LINE_PATH = 'abfss://tempdata@dha0mlp0prod.dfs.core.windows.net/lvl1/medclm_mth_line/'
# MED_CLAIMS_PATH = 'dbfs:/FileStore/tables/de/fs_temp/medclm_line_fact/'
# MED_CLAIMS_LINE_PATH = 'dbfs:/FileStore/tables/de/fs_temp/medclm_line/'

# Member Data
MEMBER_COVERAGE_DATA_PATH = '/lvl1/member_coverage_plan'
PERSON_DATA_PATH = '/lvl1/src_person'

# Customer Data
CUSTOMER_CROSSWALK_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_cust_crosswalk.csv'
CUST_INFO_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_custlvl_data.csv'

# Lookups
HRDL_MEDNDC_AGNOSTIC_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_medndcagnostic.csv'
HRDL_NDC_AGNOSTIC_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_ndcagnostic.csv'
HRDL_CPT_AGNOSTIC_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_cptagnostic.csv'
HRDL_REV_AGNOSTIC_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl5_0_revagnostic.csv'
HRDL_PFM_SIMPLE_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl_pfmsimple.csv'
RX_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/rx_lookup.csv'
REV_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/rev_manually_reduce.csv'
CPT_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/cpt_labels_v4.csv'
HCUP_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl_icd10_hcup_map.csv'

# Genetic codes lookups
ICD_GENETIC_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl_icd_genetic_codes.csv'
CPT_GENETIC_LOOKUP_PATH = 'dbfs:/FileStore/tables/de/fs_lookup/hrdl_cpt_genetic_codes.csv'
