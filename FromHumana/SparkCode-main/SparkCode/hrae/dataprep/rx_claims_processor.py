"""Module with `RxClaimsProcessor` class and the constants necessary for its calculations."""

from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, IntegerType, TimestampType, FloatType

# this imports should be commented if used in Databricks
from hrae.dataprep.fs_data_reader import read_rx_claims, read_ndc_agnostic
from hrae.dataprep.member_coverage_data_processor import MemberCoverageDataProcessor
from hrae.dataprep.processor_constants import (MIN_CLAIM_DATE, MAX_CLAIM_DATE,
                                               COMMERCIAL_PLATFORMS,
                                               MAX_HISTORICAL_PERIOD_LENGTH,
                                               INCLUDED, RX_CLAIM_CODE)
from hrae.dataprep.processor_utils import ag_allow_amt_temp_calc, times_calc
from hrae.dataprep.base_processor import BaseProcessor

# COMMAND ----------

# MAGIC %run ./fs_data_reader

# COMMAND ----------

# MAGIC %run ./processor_utils

# COMMAND ----------

# MAGIC %run ./base_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------

# MAGIC %run ./member_coverage_data_processor

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

MEMBER_COVERAGE_COLS_RX = [
    'src_rpt_cust_id',
    'src_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'member_level_incl'
]
"""List of columns taken from the member coverage data."""

NDC_AGNOSTIC_COLS_RX = [
    'ndc_id',
    'allowed_per_unit',
    'serv_unit_cnt_max'
]
"""List of columns taken from NDC agnostic lookup table."""

RX_CLAIMS_COLS = [
    'service_date',
    'process_date',
    'sdr_person_id',
    'ndc_id',
    'rx_cost',
    'payable_metric_cnt',
    'src_platform_cd',
    'src_cust_id',
    'wac_ingrd_cost_amt'
]
"""List of columns taken from the raw RxClaims data."""


class RxClaimsProcessor(BaseProcessor):
    """Class for processing raw RxClaims data taken from FS.

    `RxClaimsProcessor` is a class hired to transform RxClaims data from the raw format
    (taken from FS) to the format consumed by transformers. It uses FS source table with
    RxClaims, ndc_agnostic lookup table, and member coverage data (output of
    `MemberCoverageDataProcessor`). As the final result this processor produces DataFrame
    which contains aggregation of historical RxClaims happened within 24 months back
    from underwriting date (this value corresponds to the `MAX_HISTORICAL_PERIOD_LENGTH`
    constant) for the preselected list of members (`member_level_included` == 1).

    Attributes:
        min_date_lim (str): Minimal date starting from which claims are taken for calculation.
        max_date_lim (str): Maximal date after which claims are not taken for calculation.
        valid_platforms (List[str]): Codes of data vendors. Default value corresponds to
            Commercial claims (Medicare and Medicaid are excluded).
        rx_claims (DataFrame): Source RxClaims data.
        ndc_agnostic (DataFrame): NDC (prescription drugs) agnostic lookup table.
        member_coverage_data (DataFrame): Member coverage data (output of
            `MemberCoverageDataProcessor`).

    Example:
        The simplest way to use the processor is to call the constructor without any parameters
        and then call `run()` method. In this case minimal and maximal dates for the claims
        will be automatically taken from the constants file.

        ```py
        processed_df = RxClaimsProcessor().run()
        ```

        Also it's possible to override default claim date period defined by the constants.

        ```py
        processed_df = RxClaimsProcessor('2016-10-01', '2020-12-31').run()
        ```

        For unit-testing or some specific research it could be useful to create
        `RxClaimsProcessor` based on the custom source DataFrames instead of reading
        data from FS.

        ```py
        # Here we use the instance of `RxClaimsProcessor`
        # based on our custom DataFrame with rx claims data.
        processed_df = RxClaimsProcessor(rx_claims=custom_rx_df).run()
        ```
    """

    def __init__(self,
                 min_date_lim: str = MIN_CLAIM_DATE,
                 max_date_lim: str = MAX_CLAIM_DATE,
                 valid_platforms: List[str] = None,
                 rx_claims: DataFrame = None,
                 ndc_agnostic: DataFrame = None,
                 member_coverage_data: DataFrame = None):

        self.min_date_lim = F.to_date(F.lit(min_date_lim))
        self.max_date_lim = F.to_date(F.lit(max_date_lim))
        self.valid_platforms = valid_platforms or COMMERCIAL_PLATFORMS

        self.rx_claims = rx_claims
        self.ndc_agnostic = ndc_agnostic
        self.member_coverage_data = member_coverage_data

    def perform_initial_filtering(self, rx_claims_raw: DataFrame) -> DataFrame:
        """Performs initial filtering of the raw RxClaims data by date and source platform .

        Returns:
            DataFrame with claims happened within the defined period and related
            to appropriate source platforms.
        """
        return (
            rx_claims_raw
            .filter(
                F.col('src_platform_cd').isin(self.valid_platforms)
            )
            .filter(
                (F.col('service_date').between(self.min_date_lim, self.max_date_lim)) &
                (F.col('process_date').between(self.min_date_lim, self.max_date_lim))
            )
        )

    def calculate_agnostic_amounts(self,
                                   rx_claims: DataFrame,
                                   ndc_agnostic: DataFrame) -> DataFrame:
        """Calculates agnostic allowed amounts based on claims data and lookup tables.

        The general rules of calculation are following:

        1. Allowed amount is taken as agnostic allowed amount.
        2. If allowed amount is absent, agnostic allowed amount is calculated by the formula
        implemented in `ag_allow_amt_temp_calc` from `processor utils`.
        3. If agnostic allowed amount is more than charged amount by absolute value, charged
        amount should be used instead of agnostic allowed amount.

        Args:
            rx_claims (DataFrame): A DataFrame with RxClaims data.
            ndc_agnostic (DataFrame): NDC (prescription drugs) agnostic lookup table.

        Returns:
            DataFrame with claims data enriched by agnostic allowed amounts column.
        """
        return (
            rx_claims
            .join(ndc_agnostic, on=['ndc_id'], how='left')
            .transform(ag_allow_amt_temp_calc)
            .withColumn(
                'ag_allow_amt',
                F.coalesce(F.col('allowed_amt'), F.col('ag_allow_amt_temp'), F.lit(0.0))
            )
            .withColumn(
                'ag_allow_amt',
                F.when(
                    F.abs(F.col('ag_allow_amt')) < F.abs(F.col('charged_amt')),
                    F.col('ag_allow_amt')
                )
                .otherwise(
                    F.col('charged_amt')
                )
            )
        )

    def enrich_with_member_data(self,
                                rx_claims: DataFrame,
                                member_coverage_data: DataFrame) -> DataFrame:
        """Enriches RxClaims data with member coverage data.

        This method joins member coverage data with RxClaims. Based on the result only
        claims happened within 24 months back from underwriting date (this value corresponds
        to the `MAX_HISTORICAL_PERIOD_LENGTH` constant) are left in the output DataFrame.

        Args:
            rx_claims (DataFrame): A DataFrame with RxClaims data.
            member_coverage_data (DataFrame): A DataFrame with member coverage data (output
                of `MemberCoverageDataProcessor`).

        Returns:
            Filtered DataFrame with member coverage data included.
        """
        return (
            rx_claims
            .join(
                member_coverage_data,
                on=['sdr_person_id', 'src_cust_id'],
                how='inner'
            )
            .filter(
                (F.col('service_date') < F.col('underwriting_date')) &
                (F.col('process_date') < F.col('underwriting_date'))
            )
            .filter(
                (F.col('service_date') >=
                 F.add_months(F.col('underwriting_date'), -MAX_HISTORICAL_PERIOD_LENGTH)) &
                (F.col('process_date') >=
                 F.add_months(F.col('underwriting_date'), -MAX_HISTORICAL_PERIOD_LENGTH))
            )
            .withColumn(
                'mth_from_uw',
                F.ceil(F.months_between(F.col('underwriting_date'), F.col('service_date')))
            )
            .transform(times_calc)
        )

    def aggregate_claims(self, rx_claims: DataFrame) -> DataFrame:
        """Calculates total agnostic allowed amount per member, UW date, times and NDC id.

        This method groups RxClaims data by member id (`sdr_person_id`), customer id
        (`src_rpt_cust_id`), underwriting date, times and NDC id. Based on this grouping
        total agnostic allowed amounts are calculated.

        Returns:
            Result of RxClaims aggregation with total agnostic allowed amounts calculated.
        """
        return (
            rx_claims
            .groupby('src_rpt_cust_id', 'sdr_person_id', 'underwriting_date', 'times', 'ndc_id')
            .agg(F.sum('ag_allow_amt').alias('ag_allow_amt'))
            .withColumn(
                'ag_allow_amt',
                F.when(F.col('ag_allow_amt') > 0, F.col('ag_allow_amt')).otherwise(0)
            )
        )

    def prepare_rx_claims(self, rx_claims: DataFrame) -> DataFrame:
        """Performs initial formatting of the raw DataFrame.

        This method transforms a raw DataFrame with RxClaims data to the format convenient
        for the further use in processor. This method is a right place for column selection,
        typecasting, column renaming, etc.

        Returns:
            Formatted DataFrame.
        """
        rename_dict = {'wac_ingrd_cost_amt': 'charged_amt',
                       'rx_cost': 'allowed_amt'}
        return (
            rx_claims
            .select(RX_CLAIMS_COLS)
            .transform(lambda df: self.rename_cols(df, rename_dict))
            .withColumn('service_date', F.col('service_date').cast('date'))
            .withColumn('process_date', F.col('process_date').cast('date'))
        )

    def prepare_ndc_agnostic(self, ndc_agnostic: DataFrame) -> DataFrame:
        """Performs initial formatting of the raw DataFrame.

        This method transforms lookup table with NDC agnostic data to the format convenient
        for the further use in processor. This method is a right place for column selection,
        typecasting, column renaming, etc.

        Returns:
            Formatted DataFrame.
        """
        cast_dict = {'allowed_per_unit': 'double',
                     'serv_unit_cnt_max': 'double'}
        return (
            ndc_agnostic
            .select(NDC_AGNOSTIC_COLS_RX)
            .transform(lambda df: self.cast_types(df, cast_dict))
        )

    def prepare_member_coverage_data(self, member_coverage_data: DataFrame) -> DataFrame:
        """Performs initial formatting of the raw DataFrame.

        This method transforms a raw DataFrame with member coverage data to the format
        convenient for the further use in processor. This method is a right place for
        column selection, typecasting, column renaming, etc.

        Returns:
            Formatted DataFrame.
        """
        return (
            member_coverage_data
            .select(MEMBER_COVERAGE_COLS_RX)
            .filter(F.col('member_level_incl') == INCLUDED)
            .drop('member_level_incl')
        )

    def read_data(self) -> None:
        """Reads and prepares all source data and lookup tables used for calculations.

        If some custom DataFrames were passed to the constructor during processor creation,
        they will be considered as source DataFrames. Otherwise this method will read source
        data from the default FS location by `FsDataReader`.

        Then source DataFrames are prepared for the further usage in processor. Preparation
        usually includes some simple filtering and formatting procedures like extracting
        required columns, typecasting, column renaming, etc.
        """
        self.rx_claims = self.prepare_rx_claims(
            self.rx_claims or read_rx_claims()
        )

        self.ndc_agnostic = self.prepare_ndc_agnostic(
            self.ndc_agnostic or read_ndc_agnostic()
        )

        self.member_coverage_data = self.prepare_member_coverage_data(
            self.member_coverage_data or MemberCoverageDataProcessor().run()
        )

    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames.

        This method is created for orchestration purposes. Its main goal is to build complete
        end-to-end transformational flow starting from the raw FS data and resulting with the
        final DataFrame with result of aggregation of historical RxClaims happened within
        24 months back from underwriting date.

        Returns:
            DataFrame with result of aggregation of historical RxClaims happened within
            24 months back from underwriting date.
        """

        return (
            self.rx_claims
            .transform(self.perform_initial_filtering)
            .drop('src_platform_cd')
            .transform(
                lambda rx_claims:
                self.calculate_agnostic_amounts(rx_claims, self.ndc_agnostic)
            )
            .drop('allowed_amt', 'payable_metric_cnt', 'allowed_per_unit',
                  'serv_unit_cnt_max', 'ag_allow_amt_temp', 'charged_amt')
            .transform(
                lambda rx_claims:
                self.enrich_with_member_data(rx_claims, self.member_coverage_data)
            )
            .drop('service_date', 'process_date', 'mth_from_uw')
            .transform(self.aggregate_claims)
            .withColumn(
                'rx_sample', F.lit(RX_CLAIM_CODE)
            )
        )

    def ensure_schema(self, df: DataFrame) -> DataFrame:
        """Transforms schema of the final dataframe produced by `process()` method.

        This method uses method `cast_types()` of `BaseProcessor` with the dictionary
        which specifies expected column types for required columns.

        Returns:
            DataFrame with expected schema.
        """
        schema_dict = {
            'src_rpt_cust_id': StringType(),
            'sdr_person_id': StringType(),
            'underwriting_date': TimestampType(),
            'times': IntegerType(),
            'rx_sample': StringType(),
            'ndc_id': StringType(),
            'ag_allow_amt': FloatType()
        }
        return self.cast_types(df, schema_dict)
