"""Module with `MedClaimsProcessor` class and the constants necessary for its calculations."""

from typing import List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, IntegerType, TimestampType, FloatType

# this imports should be commented if used in Databricks
from hrae.dataprep.fs_data_reader import (read_medndc_agnostic, read_cpt_agnostic,
                                          read_rev_agnostic, read_medclm_line,
                                          read_med_claims, read_hcup_mapping,
                                          read_icd_genetic, read_cpt_genetic)
from hrae.dataprep.member_coverage_data_processor import MemberCoverageDataProcessor
from hrae.dataprep.processor_constants import (INCLUDED, NUM_MIN_DIAG_COL,
                                               NUM_MAX_DIAG_COL, DUMMY_GENETIC_CODE,
                                               COMMERCIAL_PLATFORMS, MIN_CLAIM_DATE,
                                               MAX_CLAIM_DATE, OUTPATIENT_CLAIM_CODE,
                                               INPATIENT_CLAIM_CODE, DIAGNOSIS_CODE,
                                               REVENUE_CODE, MAX_HISTORICAL_PERIOD_LENGTH,
                                               HCUP_UNKNOWN, CPT_CODE)
from hrae.dataprep.processor_utils import verticalize, times_calc, ag_allow_amt_calc
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

MEDCLM_LINE_FACT_COLS = [
  'src_cust_id',
  'medclm_key',
  'load_date',
  'serv_from_date_skey',
  'sdr_person_id',
  'src_clm_platform_cd',
  'hcpcs_cpt4_base_cd1',
  'primary_diag_cd',
  'revenue_cd',
  'allow_icob_amt',
  'chrg_amt',
  'serv_unit_cnt',
]

MEDCLM_LINE_COLS = [
  'medclm_key',
  'ndc_id'
] + [
  f'diag_cd{i}' for i in range(NUM_MIN_DIAG_COL, NUM_MAX_DIAG_COL + 1)
]

MEMBER_COVERAGE_COLS_MED = [
    'src_rpt_cust_id',
    'src_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'effective_date',
    'member_level_incl'
]

NDC_AGNOSTIC_COLS_MED = [
    'ndc_id',
    'allowed_per_unit',
    'serv_unit_cnt_max'
]

CPT_AGNOSTIC_COLS_MED = [
    'hcpcs_cpt4_base_cd1',
    'allowed_per_unit',
    'serv_unit_cnt_max'
]

REV_AGNOSTIC_COLS_MED = [
    'revenue_cd',
    'allowed_per_unit',
    'serv_unit_cnt_max'
]

DESIRED_COLS_MED = [
  'underwriting_date',
  'src_rpt_cust_id',
  'sdr_person_id',
  'med_sample',
  'datatype',
  'value',
  'times',
  'ag_allow_amt'
]


class MedClaimsProcessor(BaseProcessor):
    """Class for processing raw MedClaims data taken from FS.

    `MedClaimsProcessor` is a class hired to transform MedClaims data from the raw format
    (taken from FS) to the format consumed by transformers. It uses FS source tables with
    MedClaims data, agnostic and genetic lookup tables, and member coverage data (output of
    `MemberCoverageDataProcessor`). As the final result this processor produces DataFrame
    which contains result of verticalization and aggregation of historical MedClaims
    happened within 24 months back from underwriting date (this value corresponds to the
    `MAX_HISTORICAL_PERIOD_LENGTH` constant) for the preselected list of members
    (`member_level_included` == 1).

    Attributes:
        min_date_lim (str): Minimal date starting from which claims are taken for calculation.
        max_date_lim (str): Maximal date after which claims are not taken for calculation.
        valid_platforms (List[str]): Codes of data vendors. Default value corresponds to
            Commercial claims (Medicare and Medicaid are excluded).
        med_claims (DataFrame): Source MedClaims data (medclm_line_fact).
        medclm_line (Dataframe): DataFrame with NDC and diagnoses codes for MedClaims.
        medndc_agnostic (DataFrame): NDC (prescription drugs) agnostic lookup table.
        rev_agnostic (DataFrame): Revenue agnostic lookup table.
        cpt_agnostic (DataFrame): CPT (medical procedures) agnostic lookup table.
        member_coverage_data (DataFrame): Member coverage data (output of
            `MemberCoverageDataProcessor`).
        icd_genetic (DataFrame): Lookup table with ICD (diagnosis) codes related
            to genetic conditions.
        cpt_genetic (DataFrame): Lookup table with CPT codes related to genetic conditions.
        hcup_mapping (DataFrame): Lookup table with the mapping of ICD codes to HCUP codes
            representing higher level of ICD codes grouping.

    Example:
        The simplest way to use the processor is to call the constructor without any parameters
        and then call `run()` method. In this case minimal and maximal dates for the claims
        will be automatically taken from the constants file.

        ```py
        processed_df = MedClaimsProcessor().run()
        ```

        Also it's possible to override default claim date period defined by the constants.

        ```py
        processed_df = MedClaimsProcessor('2016-10-01', '2020-12-31').run()
        ```

        For unit-testing or some specific research it could be useful to create
        `MedClaimsProcessor` based on the custom source DataFrames instead of reading
        data from FS.

        ```py
        # Here we use the instance of `MedClaimsProcessor`
        # based on our custom DataFrame with med claims data.
        processed_df = MedClaimsProcessor(medclm_line_fact=custom_med_df).run()
        ```
    """

    def __init__(self,
                 min_date_lim: str = MIN_CLAIM_DATE,
                 max_date_lim: str = MAX_CLAIM_DATE,
                 valid_platforms: List[str] = None,
                 medclm_line: DataFrame = None,
                 medclm_line_fact: DataFrame = None,
                 medndc_agnostic: DataFrame = None,
                 cpt_agnostic: DataFrame = None,
                 rev_agnostic: DataFrame = None,
                 member_coverage_data: DataFrame = None,
                 icd_genetic: DataFrame = None,
                 cpt_genetic: DataFrame = None,
                 hcup_mapping: DataFrame = None):

        self.min_date_lim = F.to_date(F.lit(min_date_lim))
        self.max_date_lim = F.to_date(F.lit(max_date_lim))
        self.valid_platforms = valid_platforms or COMMERCIAL_PLATFORMS

        self.med_claims = medclm_line_fact
        self.medclm_line = medclm_line
        self.medndc_agnostic = medndc_agnostic
        self.cpt_agnostic = cpt_agnostic
        self.rev_agnostic = rev_agnostic
        self.member_coverage_data = member_coverage_data

        self.icd_genetic = icd_genetic
        self.cpt_genetic = cpt_genetic
        self.hcup_mapping = hcup_mapping

    def filter_dates_and_platforms(self, med_claims: DataFrame) -> DataFrame:
        """Performs initial filtering of the raw MedClaims data by date and source platform.

        Returns:
            DataFrame with claims happened within the defined period and related
            to appropriate source platforms.
        """
        return (
            med_claims
            .filter(
                F.col('src_clm_platform_cd').isin(self.valid_platforms)
            )
            .filter(
                (F.col('serv_from_date_skey').between(self.min_date_lim, self.max_date_lim)) &
                (F.col('load_date').between(self.min_date_lim, self.max_date_lim))
            )
            .drop('src_clm_platform_cd')
        )

    def join_medclm_data(self,
                         med_claims: DataFrame,
                         medclm_line: DataFrame) -> DataFrame:
        """Enriches MedClaims with NDC ids and diagnoses codes from `medclm_line` DataFrame.
        """
        return (
            med_claims.join(medclm_line, on=['medclm_key'], how='left')
        )

    def calculate_agnostic_amounts(self,
                                   med_claims: DataFrame,
                                   ndc_agnostic: DataFrame,
                                   rev_agnostic: DataFrame,
                                   cpt_agnostic: DataFrame) -> DataFrame:
        """Calculates agnostic allowed amounts based on claims data and lookup tables.

        Note:
            Agnostic allowed amounts for NDC, CPT and revenue form 3 different columns.

        Args:
            med_claims (DataFrame): A DataFrame with MedClaims data.
            ndc_agnostic (DataFrame): NDC (prescription drugs) agnostic lookup table.
            rev_agnostic (DataFrame): Revenue agnostic lookup table.
            cpt_agnostic (DataFrame): CPT (medical procedures) agnostic lookup table.

        Returns:
            DataFrame with claims data enriched by agnostic allowed amounts columns for
            NDC, CPT and revenue codes.
        """
        return (
            med_claims
            .transform(ag_allow_amt_calc('ndc_ag_allow_amt', 'ndc_id', ndc_agnostic))
            .transform(ag_allow_amt_calc('rev_ag_allow_amt', 'revenue_cd', rev_agnostic))
            .transform(ag_allow_amt_calc('cpt_ag_allow_amt', 'hcpcs_cpt4_base_cd1', cpt_agnostic))
        )

    def calculate_med_sample(self, med_claims: DataFrame) -> DataFrame:
        """Identifies inpatient/outpatient claim type based on `revenue_cd` column.

        Inpatient care generally refers to any medical service that requires admission into
        a hospital. Outpatient care stands for medical service provided that does not require
        a prolonged stay at a facility. This can include routine services such as checkups or
        visits to clinics. Even more involved procedures such as surgical procedures, so long
        as they allow you to leave the hospital or facility on the same day, can still be
        considered as outpatient care.

        Returns:
            DataFrame with claims data enriched by agnostic allowed amounts column.
        """
        return (
            med_claims
            .withColumn(
                'med_sample',
                F.when(
                    F.col('revenue_cd').isNotNull() & (F.col('revenue_cd') != ''),
                    INPATIENT_CLAIM_CODE
                )
                .otherwise(OUTPATIENT_CLAIM_CODE)
            )
        )

    def enrich_with_member_data(self,
                                med_claims: DataFrame,
                                member_coverage_data: DataFrame) -> DataFrame:
        """Enriches MedClaims data with member coverage data.

        This method joins member coverage data with MedClaims. Based on the result only
        claims happened within 24 months back from underwriting date (this value corresponds
        to the `MAX_HISTORICAL_PERIOD_LENGTH` constant) are left in the output DataFrame.

        Also for each claim `times` column is calculated.

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
                on=['sdr_person_id', 'src_cust_id'],
                how='inner'
            )
            .withColumn(
                'mth_from_uw',
                F.ceil(F.months_between(F.col('underwriting_date'), F.col('serv_from_date_skey')))
            )
            .filter(
                (F.col('serv_from_date_skey') < F.col('underwriting_date')) &
                (F.col('load_date') < F.col('underwriting_date'))
            )
            .filter(
                (F.col('serv_from_date_skey') >=
                 F.add_months(F.col('underwriting_date'), -MAX_HISTORICAL_PERIOD_LENGTH)) &
                (F.col('load_date') >=
                 F.add_months(F.col('underwriting_date'), -MAX_HISTORICAL_PERIOD_LENGTH))
            )
            .transform(times_calc)
            .drop('load_date', 'mth_from_uw')
        )

    def verticalize_data(self, med_claims: DataFrame) -> DataFrame:
        """Splits every claim on several records by CPT, revenue and diagnoses codes.

        For each unique CPT, revenue and diagnoses code in claim will be produced a separate
        record with the appropriate datatype code in the `value` column.
        """
        diag_cols = [f'diag_cd{i}' for i in range(NUM_MIN_DIAG_COL, NUM_MAX_DIAG_COL + 1)]
        coding_cols = (['revenue_cd',
                        'hcpcs_cpt4_base_cd1',
                        'ndc_id',
                        'primary_diag_cd'] + diag_cols)

        mapped = (
            med_claims
            .withColumn(
                'codes_map', verticalize(*coding_cols)
            )
            .drop(*diag_cols, 'revenue_cd', 'primary_diag_cd')
            .select(
                '*',
                F.explode('codes_map').alias('datatype', 'tmp_value')
            )
        )

        return (
            mapped
            .select('*', F.explode('tmp_value').alias('value'))
            .drop('codes_map', 'tmp_value')
        )

    def calculate_total_agnostic_amounts(self, med_claims: DataFrame) -> DataFrame:
        """Calculates final value of agnostic allowed amounts for claims after verticalization.

        Full description of calculation rules could be found in wiki (point 7):
        https://dev.azure.com/humana/Digital%20Health%20and%20Analytics/_wiki/wikis/Digital-Health-and-Analytics.wiki/6812/3.-Med-Claims-Processor
        """
        return (
            med_claims
            .withColumn(
                'ag_allow_amt',
                F.when(F.col('datatype') == DIAGNOSIS_CODE, 0)
                .otherwise(
                    # ndc_id and cpt_id are null
                    F.when(
                        (
                            (F.col('ndc_id').isNull() | (F.col('ndc_id') == ''))
                            & (F.col('hcpcs_cpt4_base_cd1').isNull() | (F.col('hcpcs_cpt4_base_cd1') == ''))
                        ),
                        F.coalesce('rev_ag_allow_amt', 'allow_icob_amt')
                    )
                    # ndc_id and cpt_id are not null
                    .otherwise(
                        F.when(F.col('datatype') == REVENUE_CODE, 0)
                        .otherwise(
                            F.coalesce('ndc_ag_allow_amt', 'cpt_ag_allow_amt', 'allow_icob_amt')
                        )
                    )
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

    def prepare_med_claims(self, med_claims: DataFrame) -> DataFrame:
        """Transforms DataFrame to the format convenient for the further usage in processor.
        """
        return (
            med_claims
            .select(MEDCLM_LINE_FACT_COLS)
            .withColumn(
                'serv_from_date_skey',
                F.to_date(F.col('serv_from_date_skey').cast('string'), 'yyyyMMdd')
            )
            .withColumn('load_date', F.col('load_date').cast('date'))
        )

    def prepare_medclm_line(self, medclm_line: DataFrame) -> DataFrame:
        """Transforms DataFrame to the format convenient for the further usage in processor.
        """
        return medclm_line.select(MEDCLM_LINE_COLS)

    def prepare_agnostic(self,
                         agnostic_df: DataFrame,
                         agnostic_cols: List) -> DataFrame:
        """Transforms DataFrame to the format convenient for the further usage in processor.

        This method is generalized for usage with lookup tables containing agnostic data.

        Args:
            agnostic_df (DataFrame): Lookup table with agnostic data.
            agnostic_cols (List[str]): List of columns to be selected from the table.
        """
        cast_dict = {'allowed_per_unit': 'double',
                     'serv_unit_cnt_max': 'double'}
        return (
            agnostic_df
            .select(agnostic_cols)
            .transform(lambda df: self.cast_types(df, cast_dict))
        )

    def prepare_member_coverage_data(self, member_coverage_data: DataFrame) -> DataFrame:
        """Transforms DataFrame to the format convenient for the further usage in processor.
        """
        return (
            member_coverage_data
            .select(MEMBER_COVERAGE_COLS_MED)
            .filter(F.col('member_level_incl') == INCLUDED)
            .drop('member_level_incl')
        )

    def prepare_hcup_mapping(self, hcup_mapping: DataFrame) -> DataFrame:
        return (
            hcup_mapping
            .select('DIAG_CD', 'prefx_map_id')
            .withColumnRenamed('DIAG_CD', 'value')
            .withColumn('datatype', F.lit(DIAGNOSIS_CODE))
        )

    def prepare_cpt_genetic(self, cpt_genetic: DataFrame) -> DataFrame:
        return (
            cpt_genetic
            .select(F.col('symp').alias('value'))
            .withColumn('datatype', F.lit(CPT_CODE))
            .withColumn('genetic_related', F.lit(True))
        )

    def prepare_icd_genetic(self, icd_genetic: DataFrame) -> DataFrame:
        return (
            icd_genetic
            .select(F.col('icd10').alias('value'))
            .withColumn('datatype', F.lit(DIAGNOSIS_CODE))
            .withColumn('genetic_related', F.lit(True))
        )
    
    def filter_unwanted_cols(self, med_claims: DataFrame) -> DataFrame:
        return med_claims.select(*DESIRED_COLS_MED)

    def replace_genetic_codes(self, med_claims: DataFrame) -> DataFrame:
        """Replaces CPT/ICD codes related to genetic conditions by the special dummy value.
        """
        genetic_codes = self.icd_genetic.union(self.cpt_genetic)

        return (
            med_claims
            .join(F.broadcast(genetic_codes), on=['datatype', 'value'], how='left')
            .withColumn(
                'value',
                F.when(F.col('genetic_related'), DUMMY_GENETIC_CODE).otherwise(F.col('value'))
            )
            .drop('genetic_related')
        )

    def replace_icd_by_hcup(self, med_claims: DataFrame) -> DataFrame:
        """Replaces ICD (diagnosis) codes by the corresponding HCUP codes.
        HCUP codes represent higher level of grouping for ICD codes.
        """
        return (
            med_claims
            .join(F.broadcast(self.hcup_mapping), on=['datatype', 'value'], how='left')
            .withColumn(
                'value',
                (F.when(F.col('datatype') == DIAGNOSIS_CODE,
                        (F.when(F.col('prefx_map_id').isNotNull() & (F.col('prefx_map_id') != ''), F.col('prefx_map_id'))
                         .otherwise(HCUP_UNKNOWN)))
                 .otherwise(F.col('value')))
            )
            .drop('prefx_map_id')
        )
      
    def aggregate_claims(self, med_claims: DataFrame) -> DataFrame:
        return (
            med_claims
            .groupby(
                'src_rpt_cust_id', 'sdr_person_id', 'underwriting_date',
                'times', 'datatype', 'value', 'med_sample'
            )
            .agg(F.sum('ag_allow_amt').alias('ag_allow_amt'))
            .withColumn(
                'ag_allow_amt',
                F.when(F.col('ag_allow_amt') > 0, F.col('ag_allow_amt')).otherwise(0)
            )
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
        self.med_claims = self.prepare_med_claims(
            self.med_claims or read_med_claims()
        )

        self.medclm_line = self.prepare_medclm_line(
            self.medclm_line or read_medclm_line()
        )

        self.medndc_agnostic = self.prepare_agnostic(
            self.medndc_agnostic or read_medndc_agnostic(),
            NDC_AGNOSTIC_COLS_MED
        )

        self.cpt_agnostic = self.prepare_agnostic(
            self.cpt_agnostic or read_cpt_agnostic(),
            CPT_AGNOSTIC_COLS_MED
        )

        self.rev_agnostic = self.prepare_agnostic(
            self.rev_agnostic or read_rev_agnostic(),
            REV_AGNOSTIC_COLS_MED
        )

        self.member_coverage_data = self.prepare_member_coverage_data(
            self.member_coverage_data or MemberCoverageDataProcessor().run()
        )

        self.hcup_mapping = self.prepare_hcup_mapping(
            self.hcup_mapping or read_hcup_mapping()
        )

        self.cpt_genetic = self.prepare_cpt_genetic(
            self.cpt_genetic or read_cpt_genetic()
        )

        self.icd_genetic = self.prepare_icd_genetic(
            self.icd_genetic or read_icd_genetic()
        )

    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames.

        This method is created for orchestration purposes. Its main goal is to build complete
        end-to-end transformational flow starting from the raw FS data and resulting with the
        final DataFrame with result of verticalization and aggregation of historical
        MedClaims happened within 24 months back from underwriting date.

        Returns:
            DataFrame with result of aggregation of historical MedClaims happened within
            24 months back from underwriting date.
        """

        return (
            self.med_claims
            .transform(self.filter_dates_and_platforms)
            .transform(
                lambda med_claims:
                self.join_medclm_data(med_claims, self.medclm_line))
            .transform(
                lambda med_claims:
                self.calculate_agnostic_amounts(med_claims,
                                                self.medndc_agnostic,
                                                self.rev_agnostic,
                                                self.cpt_agnostic)
            )
            .transform(
                lambda med_claims:
                self.enrich_with_member_data(med_claims, self.member_coverage_data)
            )
            .transform(self.calculate_med_sample)
            .transform(self.verticalize_data)
            .transform(self.calculate_total_agnostic_amounts)
            .transform(self.filter_unwanted_cols)
            .transform(self.replace_genetic_codes)
            .transform(self.replace_icd_by_hcup)
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
            'times': IntegerType(),
            'med_sample': StringType(),
            'datatype': StringType(),
            'value': StringType(),
            'ag_allow_amt': FloatType()
        }
        return self.cast_types(df, schema_dict)
