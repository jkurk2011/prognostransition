# Databricks notebook source
import os
from collections import defaultdict
from typing import Optional, Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.types import MapType, StringType, ArrayType
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.getOrCreate()

# this imports should be commented if used in Databricks
from hrae.dataprep.processor_constants import (REVENUE_CODE,
                                               CPT_CODE,
                                               DIAGNOSIS_CODE,
                                               NUM_MIN_DIAG_COL,
                                               NUM_MAX_DIAG_COL)

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------


def times_calc(df: DataFrame) -> DataFrame:
    """Implements rules for 'times' field calculation
    """
    return (
        df.withColumn('times', (F.when(F.col('mth_from_uw').between(1, 3), 1)
                                .when(F.col('mth_from_uw').between(4, 9), 2)
                                .otherwise(3)))
    )


def ag_allow_amt_temp_calc(df: DataFrame) -> DataFrame:
    """Implements custom rules to be used for ag_allow_amt calculation for RxClaims
    """
    return (
        df.withColumn(
            'ag_allow_amt_temp',
            F.signum(F.col('payable_metric_cnt'))
            * F.col('allowed_per_unit')
            * F.least(F.col('serv_unit_cnt_max'), F.abs(F.col('payable_metric_cnt')))
        )
    )


@F.udf(returnType=MapType(StringType(), ArrayType(StringType())))
def verticalize(rev_cd: str,
                cpt_id: Optional[str],
                ndc_id: Optional[str],
                primary_diag_cd: str,
                diag_cd2: str,
                diag_cd3: str,
                diag_cd4: str,
                diag_cd5: str,
                diag_cd6: str,
                diag_cd7: str,
                diag_cd8: str,
                diag_cd9: str,
                diag_cd10: str,
                diag_cd11: str,
                diag_cd12: str,
                diag_cd13: str,
                diag_cd14: str,
                diag_cd15: str,
                diag_cd16: str,
                diag_cd17: str,
                diag_cd18: str,
                diag_cd19: str,
                diag_cd20: str,
                diag_cd21: str,
                diag_cd22: str,
                diag_cd23: str,
                diag_cd24: str,
                diag_cd25: str) -> dict:
    """Implements custom rules to calculate med claims datatypes (revenue, CPT, diagnosis)"""
    output = defaultdict(list)
    if rev_cd:
        output[REVENUE_CODE].append(rev_cd)

    # In case cpt_id is not null
    if cpt_id:
        output[CPT_CODE].append(cpt_id)
    # if cpt_id is null and ndc_id is present we append null to output['mc']
    elif ndc_id:
        output[CPT_CODE].append(cpt_id)

    diag_cols = [eval('primary_diag_cd')]
    for i in range(NUM_MIN_DIAG_COL, NUM_MAX_DIAG_COL + 1):
        diag_cols.append(eval(f'diag_cd{i}'))

    if diag_cols.count(None) == len(diag_cols):
        return dict(output)
    else:
        output[DIAGNOSIS_CODE] = [diag for diag in diag_cols if diag]

    return dict(output)


def ag_allow_amt_calc(
        new_col: str,
        join_col: str,
        agnostic_df: DataFrame) -> Callable[[DataFrame], DataFrame]:
    """Implements custom rules to be used for different [ndc/cpt/rev]_ag_allow_amt calculations

    Args:
        new_col (str): Name of the column with agnostic allowed amount.
        join_col (str): Name of the column by which claim data is joined with lookup table.
        agnostic_df (DataFrame): Lookup table with agnostic data.
    """
    def _inner_func(df: DataFrame) -> DataFrame:
        return (
            df.join(agnostic_df, on=join_col, how='left')
            .withColumn(
                new_col,
                F.signum(F.col('serv_unit_cnt'))
                * F.col('allowed_per_unit')
                * F.least(F.col('serv_unit_cnt_max').cast('int'), F.abs(F.col('serv_unit_cnt')))
            )
            .drop(*['serv_unit_cnt_max', 'allowed_per_unit'])
        )

    return _inner_func


def load_cached_or_read(processor: Optional[Callable],
                        file: str,
                        cache_dir: str,
                        forced_reload: bool = False) -> DataFrame:
    """Saves processor output to DBFS.

    This function runs a processor and saves its output to DBFS. If the output already
    exists in the storage location (from previous runs), it will not be overwriten
    (this default behaviour could be changed by the `force_reload` parameter).

    Args:
        processor (BaseProcessor): An instance of processor to run.
        file (str): A name of the directory where parquet files with the output
            of the processor will be stored.
        cache_dir (str): A path to the storage directory for all processors
            outputs (is a parent folder for the `file` parameter)
        forced_reload (bool): A flag indicating will the function overwrite already
            existing output in the storage location or not.

    Returns:
        DataFrame with the processor output.
    """

    def reloaded_and_saved() -> DataFrame:
        df = processor.run().cache()
        (df.write.mode("overwrite")
         .parquet(os.path.join(cache_dir[5:], file), compression='snappy'))
        return df

    if forced_reload:
        return reloaded_and_saved()

    try:
        return (
            spark.read.parquet(
                os.path.join(cache_dir[5:], file)
            )
        )
    except AnalysisException:
        return reloaded_and_saved()
