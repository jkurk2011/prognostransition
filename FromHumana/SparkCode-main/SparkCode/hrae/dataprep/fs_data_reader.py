# Databricks notebook source
"""Parquet/CSV data reader"""

from pyspark.sql import SparkSession, DataFrame

# this import should be commented if used in Databricks
from hrae.dataprep.fs_data_reader_constants import (STORAGE_PREFIX_TMP,
                                                    STORAGE_ACCT_NAME,
                                                    RX_CLAIMS_PATH,
                                                    HRDL_MEDNDC_AGNOSTIC_PATH,
                                                    HRDL_CPT_AGNOSTIC_PATH,
                                                    HRDL_REV_AGNOSTIC_PATH,
                                                    HRDL_MEDNDC_AGNOSTIC_PATH,
                                                    HRDL_NDC_AGNOSTIC_PATH,
                                                    MEMBER_COVERAGE_DATA_PATH,
                                                    PERSON_DATA_PATH,
                                                    CUSTOMER_CROSSWALK_PATH,
                                                    HRDL_PFM_SIMPLE_PATH,
                                                    CUST_INFO_PATH,
                                                    CPT_GENETIC_LOOKUP_PATH,
                                                    ICD_GENETIC_LOOKUP_PATH,
                                                    MED_CLAIMS_PATH,
                                                    MED_CLAIMS_LINE_PATH,
                                                    HCUP_LOOKUP_PATH)

# COMMAND ----------

# MAGIC %run ./fs_data_reader_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()


def get_fs_path(storage_path: str,
                storage_prefix: str = STORAGE_PREFIX_TMP,
                storage_account: str = STORAGE_ACCT_NAME) -> str:
  """The method hides access to data storage.

  Args:
      storage_path str storage path
      storage_prefix str storage prefix
      storage_account str storage account
  Returns:
      Absolute path to data storage
  """
  return f"abfss://{storage_prefix}@{storage_account}.dfs.core.windows.net{storage_path}"


def read_rx_claims() -> DataFrame:
  """The method loads mth_rxclm_line_fact table.
      Returns:
          loaded data
  """
  return spark.read.parquet(
      get_fs_path(RX_CLAIMS_PATH)
  )


def read_med_claims() -> DataFrame:
  """The method loads mth_rxclm_line_fact table.
      Returns:
          loaded data
  """
  return spark.read.parquet(MED_CLAIMS_PATH)


def read_medclm_line() -> DataFrame:
  """The method loads mth_rxclm_line_fact table.
      Returns:
          loaded data
  """
  return spark.read.parquet(MED_CLAIMS_LINE_PATH)


def read_ndc_agnostic() -> DataFrame:
  """The method loads agnostic lookup from CSV file.
      Returns:
          loaded and transformed data
  """
  return spark.read.csv(HRDL_NDC_AGNOSTIC_PATH, header=True)


def read_medndc_agnostic() -> DataFrame:
  """The method loads agnostic lookup from CSV file.
      Returns:
          loaded and transformed data
  """
  return spark.read.csv(HRDL_MEDNDC_AGNOSTIC_PATH, header=True)


def read_cpt_agnostic() -> DataFrame:
  """The method loads agnostic lookup from CSV file.
      Returns:
          loaded and transformed data
  """
  return spark.read.csv(HRDL_CPT_AGNOSTIC_PATH, header=True)


def read_rev_agnostic() -> DataFrame:
  """The method loads agnostic lookup from CSV file.
      Returns:
          loaded and transformed data
  """
  return spark.read.csv(HRDL_REV_AGNOSTIC_PATH, header=True)


def read_coverage() -> DataFrame:
    """The method loads member_coverage_plan table.
        Returns:
            loaded data
    """
    return spark.read.parquet(
        get_fs_path(MEMBER_COVERAGE_DATA_PATH)
    )


def read_person_data() -> DataFrame:
    """The method loads src_person table.
        Returns:
            loaded data
    """
    return spark.read.parquet(
        get_fs_path(PERSON_DATA_PATH)
    )


def read_crosswalk() -> DataFrame:
    """The method loads csv mapping file with mapping to
        src_rpt_cust_id per each src_cust_id and effective date
        Returns:
            loaded data
    """
    return spark.read.csv(CUSTOMER_CROSSWALK_PATH, header=True)


def read_pfm_simple() -> DataFrame:
    """The method loads lookup table, utilized to calculate
        age_gender_factor, used for quoted_pfm_rel retrieval.
        Returns:
            loaded data
    """
    return spark.read.csv(HRDL_PFM_SIMPLE_PATH, header=True)


def read_cust_info() -> DataFrame:
    """The method loads customer level info from CSV file.
        Returns:
            loaded data
    """
    return spark.read.csv(CUST_INFO_PATH, header=True)


def read_icd_genetic() -> DataFrame:
    """Reads lookup table with ICD codes related to genetic conditions.
    """
    return spark.read.csv(ICD_GENETIC_LOOKUP_PATH, header=True)


def read_cpt_genetic() -> DataFrame:
    """Reads lookup table with CPT codes related to genetic conditions.
    """
    return spark.read.csv(CPT_GENETIC_LOOKUP_PATH, header=True)


def read_hcup_mapping() -> DataFrame:
    """Reads lookup table with icd codes to hcup codes mapping.
    """
    return spark.read.csv(HCUP_LOOKUP_PATH, header=True)
