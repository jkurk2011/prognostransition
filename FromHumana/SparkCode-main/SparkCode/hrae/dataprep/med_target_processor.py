"""Module with `MedTargetProcessor` class and the constants necessary for its calculations."""

import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType

# this imports should be commented if used in Databricks
from hrae.dataprep.med_claims_processor import MedClaimsProcessor
from hrae.dataprep.processor_constants import PLAN_YEAR_DURATION
from hrae.dataprep.processor_utils import ag_allow_amt_calc

# COMMAND ----------

# MAGIC %run ./processor_utils

# COMMAND ----------

# MAGIC %run ./med_claims_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

OUTPUT_MEMBER_COLS = [
    'src_rpt_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'effective_date',
    'cov_months_medical',
    'hist_mths_avail',
    'total_members',
    'quoted_members',
    'quoted_pfm_rel',
    'group_level_incl',
    'member_level_incl',
]
"""List of columns from MemberCoverage DataFrame to be included to the final output"""


class MedTargetProcessor(MedClaimsProcessor):
    """Class for calculating the contribution of med claims to the target variable.

    `MedTargetProcessor` is based on `MedClaimsProcessor` functionality. As the final output
    this processor calculates total med agnostic allowed amounts and total med allowed amounts
    per member and UW date to be included into total target amounts. In target calculation are
    included only med claims happened within coverage/contract/plan year.

    Attributes:
        The same as in the `MedClaimsProcessor`.

    Example:
        The simplest way to use the processor is to call the constructor without any parameters
        and then call `run()` method. In this case minimal and maximal dates for the claims
        will be automatically taken from the constants file.

        ```py
        processed_df = MedTargetProcessor().run()
        ```

        Also it's possible to override default claim date period defined by the constants.

        ```py
        processed_df = MedTargetProcessor('2016-10-01', '2020-12-31').run()
        ```

        For unit-testing or some specific research it could be useful to create
        `MedTargetProcessor` based on the custom source DataFrames instead of reading
        data from FS.

        ```py
        # Here we use the instance of `MedTargetProcessor`
        # based on our custom DataFrame with med claims data.
        processed_df = MedTargetProcessor(medclm_line_fact=custom_med_df).run()
        ```
    """

    def enrich_with_member_data(self,
                                med_claims: DataFrame,
                                member_coverage_data: DataFrame) -> DataFrame:
        """Enriches MedClaims data with member coverage data.

        This method joins member coverage data with the MedClaims. Based on the result only
        med claims happened within coverage/contract/plan year are left in the output DataFrame.

        Args:
            med_claims (DataFrame): A DataFrame with MedClaims data.
            member_coverage_data (DataFrame): A DataFrame with member coverage data (output
                of `MemberCoverageDataProcessor`).

        Returns:
            Filtered DataFrame with member coverage data included.
        """
        return (
            med_claims
            .join(
                member_coverage_data,
                on=['sdr_person_id', 'src_cust_id'], how='inner'
            )
            .filter(
                (F.col('serv_from_date_skey') <
                 F.add_months(F.col('effective_date'), PLAN_YEAR_DURATION)) &
                (F.col('load_date') <
                 F.add_months(F.col('effective_date'), PLAN_YEAR_DURATION)) &
                (F.col('serv_from_date_skey') >= F.col('effective_date')) &
                (F.col('load_date') >= F.col('effective_date'))
            )
        )

    def prepare_member_coverage_data(self, member_coverage_data: DataFrame) -> DataFrame:
        """Performs initial formatting of the raw DataFrame.

        This method transforms a raw DataFrame with member coverage data to the
        format convenient for further use in the processor. This method is a right place
        for column selection, typecasting, renaming, etc.

        Args:
            member_coverage_data (DataFrame): A DataFrame with raw member coverage data (output
                of `MemberCoverageDataProcessor`).

        Returns:
            Formatted DataFrame.
        """
        return (
            member_coverage_data
            .select(OUTPUT_MEMBER_COLS + ['src_cust_id'])
        )

    def calculate_agnostic_amounts(self,
                                   med_claims: DataFrame,
                                   ndc_agnostic: DataFrame,
                                   rev_agnostic: DataFrame,
                                   cpt_agnostic: DataFrame) -> DataFrame:
        """Calculates agnostic allowed amounts based on claims data and lookup tables.

        Args:
            med_claims (DataFrame): A DataFrame with MedClaims data.
            ndc_agnostic (DataFrame): NDC (prescription drugs) agnostic lookup table.
            rev_agnostic (DataFrame): Revenue agnostic lookup table.
            cpt_agnostic (DataFrame): CPT (medical procedures) agnostic lookup table.

        Returns:
            DataFrame with claims data enriched by agnostic allowed amounts for
            NDC, CPT and revenue codes.
        """
        return (
            med_claims
            .transform(ag_allow_amt_calc('ndc_ag_allow_amt', 'ndc_id', ndc_agnostic))
            .transform(ag_allow_amt_calc('rev_ag_allow_amt', 'revenue_cd', rev_agnostic))
            .transform(ag_allow_amt_calc('cpt_ag_allow_amt', 'hcpcs_cpt4_base_cd1', cpt_agnostic))
            .withColumn(
                'ag_allow_amt',
                F.coalesce(
                    'ndc_ag_allow_amt', 'cpt_ag_allow_amt',
                    'rev_ag_allow_amt', 'allow_icob_amt'
                )
            )
            .withColumn(
                'ag_allow_amt',
                F.when(
                    F.abs(F.col('ag_allow_amt')) < F.abs(F.col('chrg_amt')),
                    F.col('ag_allow_amt')
                )
                .otherwise(F.col('chrg_amt'))
            )
        )

    def aggregate_claims(self, med_claims: DataFrame) -> DataFrame:
        """Calculates target metrics per member and UW date.

        This method calculates total med agnostic allowed amounts and total med allowed amounts
        per member and UW date to be included into total target amounts.

        Note:
            med_ prefix in column names is added to resolve the ambiguity that occurs when this
            data is joined with the `RxTargetProcessor` output in the `TargetClaimsProcessor`

        Args:
            med_claims (DataFrame): A DataFrame with MedClaims data.

        Returns:
            Result of MedClaims aggregation.
        """
        return (
            med_claims
            .groupby(
                'src_rpt_cust_id', 'sdr_person_id',
                'underwriting_date', 'effective_date'
            )
            .agg(
                F.sum('ag_allow_amt').alias('total_med_ag_allow_amt'),
                F.sum('allow_icob_amt').alias('total_med_allow_amt'),
                F.max('cov_months_medical').alias('med_cov_months_medical'),
                F.max('hist_mths_avail').alias('med_hist_mths_avail'),
                F.max('total_members').alias('med_total_members'),
                F.max('quoted_members').alias('med_quoted_members'),
                F.max('quoted_pfm_rel').alias('med_quoted_pfm_rel'),
                F.max('member_level_incl').alias('med_member_level_incl'),
                F.max('group_level_incl').alias('med_group_level_incl')
            )
            .withColumn(
                'total_med_ag_allow_amt',
                (F.when(F.col('total_med_ag_allow_amt') > 0,
                        F.col('total_med_ag_allow_amt'))
                 .otherwise(0))
            )
        )

    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames.

        This method is created for orchestration purposes. Its main goal is to build complete
        end-to-end transformational flow starting from the raw source data and resulting
        with the final DataFrame.

        Returns:
            Processed data.
        """

        return (
            self.med_claims
            .transform(self.filter_dates_and_platforms)
            .transform(
                lambda med_claims:
                self.enrich_with_member_data(med_claims, self.member_coverage_data)
            )
            .drop('src_cust_id', 'serv_from_date_skey', 'load_date')
            .join(
                self.medclm_line.select('medclm_key', 'ndc_id'),
                on=['medclm_key'],
                how='left'
            )
            .transform(
                lambda med_claims:
                self.calculate_agnostic_amounts(
                    med_claims, self.medndc_agnostic, self.rev_agnostic, self.cpt_agnostic
                )
            )
            .select(['ag_allow_amt', 'allow_icob_amt'] + OUTPUT_MEMBER_COLS)
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
            'med_hist_mths_avail': IntegerType(),
            'med_cov_months_medical': IntegerType(),
            'med_quoted_members': IntegerType(),
            'med_quoted_pfm_rel': FloatType(),
            'med_total_members': IntegerType(),
            'total_med_ag_allow_amt': FloatType(),
            'total_med_allow_amt': FloatType()
        }
        return self.cast_types(df, schema_dict)
