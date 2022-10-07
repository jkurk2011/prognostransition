"""Module with `RxTargetProcessor` class and the constants necessary for its calculations."""

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType

# this imports should be commented if used in Databricks
from hrae.dataprep.processor_constants import PLAN_YEAR_DURATION
from hrae.dataprep.rx_claims_processor import RxClaimsProcessor, MEMBER_COVERAGE_COLS_RX

# COMMAND ----------

# MAGIC %run ./rx_claims_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

PERIPHERAL_MEMBER_COLS = [
    'effective_date',
    'cov_months_medical',
    'hist_mths_avail',
    'total_members',
    'quoted_members',
    'quoted_pfm_rel',
    'group_level_incl'
]
"""List of columns which are not involved into the calculations
but must be present in the final output.
"""


class RxTargetProcessor(RxClaimsProcessor):
    """Class for calculating the contribution of rx claims to the target variable.

    `RxTargetProcessor` is inherited from `RxClaimsProcessor` and mostly based on its
    functionality, overriding only some methods. As the final output this processor
    calculates total rx agnostic allowed amounts and total rx allowed amounts per member
    and UW date to be included into total target amounts. In target calculation are
    included only rx claims happened within coverage/contract/plan year per member and UW date.

    Attributes:
        The same as in the `RxClaimsProcessor`.

    Example:
        The simplest way to use the processor is to call the constructor without any parameters
        and then call `run()` method. In this case minimal and maximal dates for the claims
        will be automatically taken from the constants file.

        ```py
        processed_df = RxTargetProcessor().run()
        ```

        Also it's possible to override default claim date period defined by the constants.

        ```py
        processed_df = RxTargetProcessor('2016-10-01', '2020-12-31').run()
        ```

        For unit-testing or some specific research it could be useful to create
        `RxTargetProcessor` based on the custom source DataFrames instead of reading
        data from FS.

        ```py
        # Here we use the instance of `RxTargetProcessor`
        # based on our custom DataFrame with rx claims data.
        processed_df = RxTargetProcessor(rx_claims=custom_rx_df).run()
        ```
    """

    def enrich_with_member_data(self,
                                rx_claims: DataFrame,
                                member_coverage_data: DataFrame) -> DataFrame:
        """Enriches RxClaims data with member coverage data.

        This method joins member coverage data with RxClaims. Based on the result only
        claims happened within coverage/contract/plan year are left in the output DataFrame.

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
                on=['sdr_person_id', 'src_cust_id'], how='inner'
            )
            .filter(
                (F.col('service_date') <
                 F.add_months(F.col('effective_date'), PLAN_YEAR_DURATION)) &
                (F.col('process_date') <
                 F.add_months(F.col('effective_date'), PLAN_YEAR_DURATION)) &
                (F.col('service_date') >= F.col('effective_date')) &
                (F.col('process_date') >= F.col('effective_date'))
            )
        )

    def aggregate_claims(self, rx_claims: DataFrame) -> DataFrame:
        """Calculates target metrics per member and UW date.

        This method calculates total rx agnostic allowed amounts and total rx allowed amounts
        per member and UW date to be included into total target amounts.

        Note:
            rx_ prefix in column names is added to resolve the ambiguity that occurs when this
            data is joined with the `MedTargetProcessor` output in the `TargetClaimsProcessor`

        Args:
            rx_claims (DataFrame): A DataFrame with RxClaims data.

        Returns:
            Result of RxClaims aggregation.
        """
        return (
            rx_claims
            .groupby(
                'src_rpt_cust_id', 'sdr_person_id',
                'underwriting_date', 'effective_date'
            )
            .agg(
                F.sum('ag_allow_amt').alias('total_rx_ag_allow_amt'),
                F.sum('allowed_amt').alias('total_rx_allow_amt'),
                F.max('cov_months_medical').alias('rx_cov_months_medical'),
                F.max('hist_mths_avail').alias('rx_hist_mths_avail'),
                F.max('total_members').alias('rx_total_members'),
                F.max('quoted_members').alias('rx_quoted_members'),
                F.max('quoted_pfm_rel').alias('rx_quoted_pfm_rel'),
                F.max('member_level_incl').alias('rx_member_level_incl'),
                F.max('group_level_incl').alias('rx_group_level_incl')
            )
            .withColumn(
                'total_rx_ag_allow_amt',
                (F.when(F.col('total_rx_ag_allow_amt') > 0,
                        F.col('total_rx_ag_allow_amt'))
                 .otherwise(0))
            )
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
            .select(MEMBER_COVERAGE_COLS_RX + PERIPHERAL_MEMBER_COLS)
        )

    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames.

        This method is created for orchestration purposes. Its main goal is to build complete
        end-to-end transformational flow starting from the raw source data and resulting
        with the final DataFrame with total rx agnostic allowed amounts and total
        rx allowed amounts per member and UW date.

        Returns:
            DataFrame with total rx agnostic allowed amounts and total
            rx allowed amounts per member and UW date.
        """

        return (
            self.rx_claims
            .transform(self.perform_initial_filtering)
            .drop('src_platform_cd')
            .transform(
                lambda rx_claims:
                self.calculate_agnostic_amounts(rx_claims, self.ndc_agnostic)
            )
            .drop('payable_metric_cnt', 'allowed_per_unit', 'serv_unit_cnt_max',
                  'ag_allow_amt_temp', 'charged_amt')
            .transform(
                lambda rx_claims:
                self.enrich_with_member_data(rx_claims, self.member_coverage_data)
            )
            .drop('service_date', 'process_date')
            .transform(self.aggregate_claims)
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
            'rx_hist_mths_avail': IntegerType(),
            'rx_cov_months_medical': IntegerType(),
            'rx_quoted_members': IntegerType(),
            'rx_quoted_pfm_rel': FloatType(),
            'rx_total_members': IntegerType(),
            'total_rx_ag_allow_amt': FloatType(),
            'total_rx_allow_amt': FloatType()
        }
        return self.cast_types(df, schema_dict)
