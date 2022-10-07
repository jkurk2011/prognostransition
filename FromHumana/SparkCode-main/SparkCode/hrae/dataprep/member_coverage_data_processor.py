from typing import List

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql.types import IntegerType, DateType, StringType, TimestampType, FloatType, ArrayType
from datetime import date

# this imports should be commented if used in Databricks
from hrae.dataprep.fs_data_reader import read_coverage, read_crosswalk, read_person_data, read_pfm_simple
from hrae.dataprep.processor_constants import (MIN_EFF_DATE, MAX_EFF_DATE,
                                               COMMERCIAL_PLATFORMS, PLAN_YEAR_DURATION,
                                               DEPENDENT, SUBSCRIBER, SUBSCRIBER_REL_TYPE,
                                               MAX_HISTORICAL_PERIOD_LENGTH,
                                               MIN_HISTORICAL_PERIOD_LENGTH,
                                               DAYS_IN_YEAR_AVG, INCLUDED, EXCLUDED,
                                               MIN_COV_MONTH_MEDICAL)
from hrae.dataprep.base_processor import BaseProcessor

# COMMAND ----------

# MAGIC %run ./fs_data_reader

# COMMAND ----------

# MAGIC %run ./base_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

first_day_of_month = F.udf(lambda x: date(x.year, x.month, 1), DateType())
get_list = F.udf(lambda min_mbr_age, max_mbr_age: list(range(min_mbr_age, max_mbr_age + 1)), ArrayType(IntegerType()))

MEMBER_COVERAGE_COLS = [
    'src_cust_id',
    'sdr_person_id',
    'src_platform_cd',
    'cov_eff_date',
    'cov_end_date',
    'rel_type_cd'
]

PERSON_DATA_COLS = [
    'sdr_person_id',
    'src_platform_cd',
    'sex_cd',
    'birth_date'
]

QUOTED_MEMBERS_AND_PFM_COLS = [
    'src_rpt_cust_id',
    'effective_date',
    'cov_eff_date',
    'sdr_person_id',
    'age_gender_factor'
]

TOTAL_MEMBERS_COLS = [
    'src_rpt_cust_id',
    'effective_date',
    'sdr_person_id'
]

NOT_NEEDED_ANYMORE_COLS = [
    'subscriber_flag',
    'birth_date',
    'sex_cd',
    'age',
    'min_mbr_age',
    'max_mbr_age'
]


class MemberCoverageDataProcessor(BaseProcessor):
    """MemberCoverageDataProcessor is a class that produces the member coverage
        data and eligibility rules for member and group levels
        Example:
            MemberCoverageDataProcessor().run()
            MemberCoverageDataProcessor('2018-01-01', '2020-01-01').run()
        Attributes:
            min_date_lim (str): For calculations are taken only members
                with effective date between min_date_lim and max_date_lim.
            max_date_lim (str): For calculations are taken only members
                with effective date between min_date_lim and max_date_lim.
            valid_platforms (List[str]): Codes of data vendors. Default value corresponds to
                commercial claims (Medicare and Medicaid are excluded).
            crosswalk (Dataframe): Custom DataFrame with customer crosswalk data.
                By default data is taken from csv file.
            coverage (Dataframe): Custom DataFrame with member coverage data.
                By default data is taken from FS.
            person_data (Dataframe): Custom DataFrame with member personal data.
                By default data is taken from FS.
            interim_data (Dataframe): Custom DataFrame with interim data.
                By default data is calculated using crosswalk, coverage, person_data.
            pfm (Dataframe): Custom DataFrame with pfm data.
                By default data is taken from FS.
    """

    def __init__(self,
                 min_date_lim: str = MIN_EFF_DATE,
                 max_date_lim: str = MAX_EFF_DATE,
                 valid_platforms: List[str] = None,
                 crosswalk: DataFrame = None,
                 coverage: DataFrame = None,
                 person_data: DataFrame = None,
                 interim_data: DataFrame = None,
                 pfm: DataFrame = None):
        self.min_date_lim = min_date_lim
        self.max_date_lim = max_date_lim
        self.valid_platforms = valid_platforms or COMMERCIAL_PLATFORMS

        self.crosswalk = crosswalk
        self.coverage = coverage
        self.person_data = person_data
        self.interim_data = interim_data
        self.pfm = pfm

    def prepare_crosswalk(self, crosswalk: DataFrame) -> DataFrame:
        """Method prepare_crosswalk extracts required columns from data frame,
            truncates effective date to the 1st day of the month,
            filters by effective date the data,
            takes max src_rpt_cust_id in case of data quality issues.
        Args:
            crosswalk (Dataframe): Custom DataFrame with customer crosswalk data.
        Returns:
            data frame for joining by method read_data().
        """
        return (
            crosswalk
            .withColumn('effective_date', first_day_of_month(F.to_date('lastrenwldt_cln', 'ddMMMyyyy')))
            .filter(F.col('effective_date').between(self.min_date_lim, self.max_date_lim))
            .groupby('src_cust_id', 'effective_date')
            .agg(F.max('src_rpt_cust_id').alias('src_rpt_cust_id'))
        )

    def prepare_coverage(self, coverage: DataFrame) -> DataFrame:
        """Method prepare_coverage extracts required columns from data frame,
            filters data by src_platform_cd so that only commercial members would be taken,
            calculates subscriber_flag column using rel_type_cd column,
            calculates cov_eff_date and cov_end_date as boundary for each member and customer.
        Args:
            coverage (Dataframe): Custom DataFrame with member coverage data.
        Returns:
            data frame for joining by method read_data().
        """
        return (
            coverage
            .select(MEMBER_COVERAGE_COLS)
            .filter(F.col('src_platform_cd').isin(self.valid_platforms))
            .withColumn(
                'subscriber_flag',
                F.when(
                    F.col('rel_type_cd') == SUBSCRIBER_REL_TYPE,
                    SUBSCRIBER
                )
                .otherwise(DEPENDENT)
            )
            .groupby('sdr_person_id', 'src_cust_id')
            .agg(F.min('cov_eff_date').cast(DateType()).alias('cov_eff_date'),
                 F.max('cov_end_date').cast(DateType()).alias('cov_end_date'),
                 F.max('subscriber_flag').alias('subscriber_flag'))
        )

    def prepare_person_data(self, person_data: DataFrame) -> DataFrame:
        """Method prepare_person_data extracts required columns from data frame,
            filters data by src_platform_cd so that only commercial members would be taken,
            takes max sex_cd and birth_date in case of data quality issues.
        Args:
            person_data (Dataframe): Custom DataFrame with member personal data.
        Returns:
            data frame for joining by method read_data().
        """
        return (
            person_data
            .select(PERSON_DATA_COLS)
            # TODO: clarify a possibility to generate exception if the platform is not correct
            .filter((F.col('src_platform_cd').isin(self.valid_platforms)))
            .groupby('sdr_person_id')
            .agg(F.max('sex_cd').alias('sex_cd'),
                 F.max('birth_date').alias('birth_date'))
            # sex_cd must be F or M. If it has another value, it must be changed to M
            .withColumn('sex_cd', F.when(F.col('sex_cd') != 'F', 'M').otherwise('F'))
        )

    def read_data(self) -> None:
        """Method read_data reads parquet and csv data sources by FsDataReader,
            extracts required columns from data frame,
            filters the data before joining (e.g. based on underwriting date),
            transforms column names to lower case or even rename them,
            prepares data frame for final processing by method process().
        """
        if not self.interim_data:
            self.crosswalk = self.prepare_crosswalk(
                self.crosswalk or read_crosswalk()
            )
            self.coverage = self.prepare_coverage(
                self.coverage or read_coverage()
            )
            self.person_data = self.prepare_person_data(
                self.person_data or read_person_data()
            )

            self.interim_data = self.prepare_interim_data()

        self.pfm = self.prepare_pfm(
            self.pfm or read_pfm_simple()
        )

    def prepare_interim_data(self) -> DataFrame:
        """Method prepare_interim_data joins crosswalk, coverage and person_data data frames,
        filters data by cov_eff_date and cov_end_date,
        Max of cov_end_date and min of cov_eff_date should be taken since 1 member can be present
        with 2 and more src_cust_ids grouped into 1 src_rpt_cust_id and within these dates coverage
        is to be considered continuous,
        sets subscriber_flag to 1 for sdr_person_id in src_rpt_cust_id if at least for one src_cust_id is 1.
        Returns:
             joined data frame for processing by method process().
        """
        window = Window.partitionBy('src_rpt_cust_id', 'sdr_person_id')
        return (
            self.crosswalk
            .join(self.coverage, 'src_cust_id')
            .withColumn('cov_eff_date', F.min('cov_eff_date').over(window))
            .withColumn('cov_end_date', F.max('cov_end_date').over(window))
            .withColumn('subscriber_flag', F.max('subscriber_flag').over(window))
            .filter(
                (F.col('effective_date').between(
                    F.add_months(F.col('cov_eff_date'), -PLAN_YEAR_DURATION),
                    F.col('cov_end_date'))) &
                (F.col('sdr_person_id').isNotNull() & (F.col('sdr_person_id') != ''))
            )
            .join(self.person_data, 'sdr_person_id', 'left')
        )

    def enrich_with_hist_and_cov_months_columns(self, interim_data: DataFrame) -> DataFrame:
        """Method enrich_with_hist_and_cov_months_columns
            calculates hist_mths_avail as difference in months between underwriting_date and cov_eff_date
            or 24 months if difference is more than 24, or 0 if less than 0,
            calculates cov_months_medical as difference in months between min(effective_date + 1 year, cov_end_date)
            and max(effective_date, cov_eff_date).
        Args:
            interim_data (Dataframe): Custom DataFrame with interim data.
        Returns:
            interim data frame with columns underwriting_date, hist_mths_avail, cov_months_medical and subscriber_flag.
        """
        hist = F.months_between(F.col('underwriting_date'), F.col('cov_eff_date')).cast(IntegerType())
        hist = (F.when(hist > MAX_HISTORICAL_PERIOD_LENGTH, MAX_HISTORICAL_PERIOD_LENGTH)
                 .when(hist < MIN_HISTORICAL_PERIOD_LENGTH, MIN_HISTORICAL_PERIOD_LENGTH)
                 .otherwise(hist))
        cov_end_date = F.date_add(F.col('cov_end_date'), 1)
        end_of_plan_year = F.add_months(F.col('effective_date'), 12)
        cov_months = F.months_between(
            F.when(cov_end_date < end_of_plan_year, cov_end_date).otherwise(end_of_plan_year),
            F.when(F.col('cov_eff_date') > F.col('effective_date'), F.col('cov_eff_date'))
             .otherwise(F.col('effective_date'))
        ).cast(IntegerType())
        return (
            interim_data
            .withColumn('hist_mths_avail', hist)
            .withColumn('cov_months_medical', cov_months)
        )

    def prepare_pfm(self, pfm: DataFrame) -> DataFrame:
        """Method prepare_pfm
            creates a new row for each age between min_mbr_age and max_mbr_age with column age to use it in join,
            drops columns that will not be needed anymore,
            renames subscriber_ind to subscriber_flag,
            Max age_gender_factor is taken in case of data quality issues.
        Args:
            pfm (Dataframe): Custom DataFrame with pfm data.
        Returns:
            data frame for joining by method enrich_with_age_gender_factor().
        """
        return (
            pfm
            .withColumn('list', get_list(F.col('min_mbr_age').cast(IntegerType()),
                                         F.col('max_mbr_age').cast(IntegerType())))
            .withColumn('age', F.explode('list'))
            .drop('min_mbr_age', 'max_mbr_age', 'list')
            .withColumnRenamed('subscriber_ind', 'subscriber_flag')
            .groupby('sex_cd', 'subscriber_flag', 'age')
            .agg(F.max('age_gender_factor').alias('age_gender_factor'))
        )

    def enrich_with_age_gender_factor(self,
                                      interim_data: DataFrame,
                                      pfm: DataFrame) -> DataFrame:
        """Method enrich_with_age_gender_factor
            calculates age as difference between effective_date and birth_date,
            gets age_gender_factor column by joining pfm lookup table.
        Args:
            interim_data (Dataframe): Custom DataFrame with interim data.
            pfm (Dataframe): Custom DataFrame with pfm data.
        Returns:
            interim data frame with age_gender_factor.
        """
        age = F.floor(F.datediff('effective_date', 'birth_date') / DAYS_IN_YEAR_AVG).cast(IntegerType())
        return (
            interim_data
            # age must be a non-negative value to get age_gender_factor
            .withColumn('age', F.when(age > 0, age).otherwise(0))
            .join(pfm, ['sex_cd', 'subscriber_flag', 'age'], 'left')
        )

    def get_quoted_members_and_pfm(self, interim_data: DataFrame = None) -> DataFrame:
        """Method get_quoted_members_and_pfm
            filters quoted members by cov_eff_date <= effective_date,
            calculates quoted_members as count of quoted members,
            calculates quoted_pfm_rel as mean of age_gender_factor.
        Args:
            interim_data (Dataframe): Custom DataFrame with interim data.
        Returns:
            data frame with quoted_members and quoted_pfm_rel.
        """
        return (
            interim_data
            .select(QUOTED_MEMBERS_AND_PFM_COLS)
            .filter(F.col('effective_date') >= F.col('cov_eff_date'))
            .select('src_rpt_cust_id', 'effective_date', 'sdr_person_id', 'age_gender_factor').distinct()
            .groupby('src_rpt_cust_id', 'effective_date')
            .agg(F.count('sdr_person_id').alias('quoted_members'),
                 F.mean('age_gender_factor').alias('quoted_pfm_rel'))
        )

    def get_total_members(self, interim_data: DataFrame = None) -> DataFrame:
        """Method get_total_members
            calculates total_members as count of all members.
        Args:
            interim_data (Dataframe): Custom DataFrame with interim data.
        Returns:
            data frame with total_members.
        """
        return (
            interim_data
            .select(TOTAL_MEMBERS_COLS).distinct()
            .groupby('src_rpt_cust_id', 'effective_date')
            .agg(F.count('sdr_person_id').alias('total_members'))
        )

    def process(self) -> DataFrame:
        """Method process enriches joined data frame from read_data() with all needed columns
            and filters data so that only members with coverage greater than 6 months stay.
            All calculation rules can be found in
            [Member coverage Processor](https://dev.azure.com/humana/Digital%20Health%20and%20Analytics/_wiki/wikis/Digital-Health-and-Analytics.wiki/6810/1.-Member-coverage-data-Processor)
        Returns:
            processed data
        """
        member_coverage_data = (
            self.interim_data
            .withColumn('underwriting_date', F.add_months('effective_date', -3).cast(TimestampType()))
            .transform(self.enrich_with_hist_and_cov_months_columns)
            .transform(
                lambda interim_data:
                self.enrich_with_age_gender_factor(interim_data, self.pfm)
            )
            .drop(*NOT_NEEDED_ANYMORE_COLS)
            .cache()
        )
        total_members = self.get_total_members(member_coverage_data)
        quoted_members_and_pfm = self.get_quoted_members_and_pfm(member_coverage_data)
        return (
            member_coverage_data
            .drop('age_gender_factor')
            .filter(F.col('cov_months_medical') > MIN_COV_MONTH_MEDICAL)
            .join(total_members, ['src_rpt_cust_id', 'effective_date'], 'left')
            .join(quoted_members_and_pfm, ['src_rpt_cust_id', 'effective_date'], 'left')
            .withColumn(
                'member_level_incl',
                F.when(F.col('hist_mths_avail') > MIN_HISTORICAL_PERIOD_LENGTH, INCLUDED)
                .otherwise(EXCLUDED)
            )
            .withColumn('group_level_incl', F.lit(INCLUDED))
            # if all members joined to customer after effective_date then quoted_members=0 and quoted_pfm_rel=null
            .na.fill({'quoted_members': 0})
        )

    def ensure_schema(self, df: DataFrame) -> DataFrame:
        """Method ensure_schema transforms schema of final dataframe from process() method.
        It uses method cast_types() of BaseProcessor with defined schema_dict.
        schema_dict specifies column types for required columns only.
        Returns:
            data with correct schema
        """
        schema_dict = {
            'src_rpt_cust_id': StringType(),
            'sdr_person_id': StringType(),
            'underwriting_date': TimestampType(),
            'hist_mths_avail': IntegerType(),
            'cov_months_medical': IntegerType(),
            'quoted_members': IntegerType(),
            'quoted_pfm_rel': FloatType(),
            'total_members': IntegerType()
        }
        return self.cast_types(df, schema_dict)
