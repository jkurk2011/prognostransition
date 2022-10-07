from pyspark.sql import SparkSession, DataFrame

# this import should be commented if used in Databricks
from hrae.dataprep.fs_data_reader_constants import (RX_LOOKUP_PATH, REV_LOOKUP_PATH,
                                                    CPT_LOOKUP_PATH, HCUP_LOOKUP_PATH)

# COMMAND ----------

# MAGIC %run ./fs_data_reader_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

RX_LOOKUP_COLS = [
  'ndc_id',
  'pref_map_id',
  'gpi_drug_group_desc'
]

REV_LOOKUP_COLS = [
  'value',
  'prefx_map_id',
  'long_desc'
]

CPT_LOOKUP_COLS = [
  'value',
  'prefx_map_id',
  'long_desc'
]

HCUP_LOOKUP_COLS = [
  'DIAG_CD',
  'prefx_map_id'
]


def read_rx_lookup() -> DataFrame:
    """The method loads rx lookup table and extracts required columns from data frame.
    Returns:
        loaded data
    """
    return (
        spark.read
        .csv(RX_LOOKUP_PATH, header=True)
        .select(RX_LOOKUP_COLS)
    )


def read_rev_lookup() -> DataFrame:
    """The method loads rev lookup table and extracts required columns from data frame.
    Returns:
        loaded data
    """
    return (
        spark.read
        .csv(REV_LOOKUP_PATH, header=True)
        .select(REV_LOOKUP_COLS)
    )


def read_cpt_lookup() -> DataFrame:
    """The method loads cpt lookup table and extracts required columns from data frame.
    Returns:
        loaded data
    """
    return (
        spark.read
        .csv(CPT_LOOKUP_PATH, header=True)
        .select(CPT_LOOKUP_COLS)
    )


def read_hcup_lookup() -> DataFrame:
    """Reads lookup table with icd codes to hcup codes mapping and extracts required columns.
    """
    return (
        spark.read
        .csv(HCUP_LOOKUP_PATH, header=True)
        .select('DIAG_CD', 'prefx_map_id')
        .withColumnRenamed('DIAG_CD', 'value')
    )
