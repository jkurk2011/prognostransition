import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType, StringType, IntegerType, TimestampType, FloatType
from datetime import date

# this imports should be commented if used in Databricks
from hrae.dataprep.fs_data_reader import read_cust_info
from hrae.dataprep.base_processor import BaseProcessor
from hrae.dataprep.processor_constants import (MIN_EFF_DATE, MAX_EFF_DATE, INCLUDED,
                                               MAX_CLAIM_DATE)
from hrae.dataprep.target_claims_processor import TargetClaimsProcessor

# COMMAND ----------

# MAGIC %run ./fs_data_reader

# COMMAND ----------

# MAGIC %run ./base_processor

# COMMAND ----------

# MAGIC %run ./target_claims_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------


spark = SparkSession.builder.getOrCreate()

first_day_of_month = F.udf(lambda x: date(x.year, x.month, 1), DateType())

CUST_INFO_COLS_MEMBER = [
    'src_rpt_cust_id',
    'effective_date',
    'train_subpartition',
    'comm_rt',
    'lf_group',
    'aso_group',
    'bus_segment_cd'
]

TARGET_CLAIMS_DATA_COLS_MEMBER = [
    'src_rpt_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'effective_date',
    'hist_mths_avail',
    'quoted_members',
    'quoted_pfm_rel',
    'member_level_incl',
    'mbr_mth_ag_cmpl_allow'
]

INTERIM_COLS = [
    'src_rpt_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'train_subpartition',
    'bus_segment_cd',
    'comm_rt',
    'lf_group',
    'aso_group',
    'mbr_mth_ag_cmpl_allow'
]


class MemberDataProcessor(BaseProcessor):
    """MemberDataProcessor is a class that produces the member data
        Example:
            MemberDataProcessor().run()
            MemberDataProcessor('2018-01-01', '2020-01-01').run()
        Attributes:
            min_date_lim (str): For calculations are taken only members
                with effective date between min_date_lim and max_date_lim.
            max_date_lim (str): For calculations are taken only members
                with effective date between min_date_lim and max_date_lim.
            cust_info (Dataframe): Custom DataFrame with customer level data.
                By default data is taken from csv file.
            target_claims_data (Dataframe): Custom DataFrame with target claims data.
                By default data is calculated with help of TargetClaimsProcessor.
            interim_data (Dataframe): Custom DataFrame with interim data.
                By default data is calculated using cust_info and target_claims_data.
    """

    def __init__(self,
                 min_date_lim: str = MIN_EFF_DATE,
                 max_date_lim: str = MAX_EFF_DATE,
                 cust_info: DataFrame = None,
                 target_claims_data: DataFrame = None,
                 interim_data: DataFrame = None):
        self.min_date_lim = min_date_lim
        self.max_date_lim = max_date_lim

        self.cust_info = cust_info
        self.target_claims_data = target_claims_data
        self.interim_data = interim_data

    def prepare_cust_info(self, cust_info: DataFrame) -> DataFrame:
        """Method prepare_cust_info extracts required columns from data frame,
            truncates effective date to the 1st day of the month,
            filters by effective date the data,
            renames partition column into train_subpartition.
        Args:
            cust_info (Dataframe): Custom DataFrame with customer level data.
        Returns:
            data frame for joining by method read_data().
        """
        return (
            cust_info
            .withColumn('effective_date', first_day_of_month(F.to_date('lastrenwldt_cln', 'ddMMMyyyy')))
            .filter(F.col('effective_date').between(self.min_date_lim, self.max_date_lim))
            .withColumnRenamed('partition', 'train_subpartition')
            .select(CUST_INFO_COLS_MEMBER)
            .na.fill({
                'aso_group': 0,
                'lf_group': 0,
                'comm_rt': 0
            })
        )

    def prepare_target_claims_data(self, target_claims_data: DataFrame) -> DataFrame:
        """Method prepare_target_claims_data extracts required columns from data frame,
            filters data by member_level_incl = 1,
            drops member_level_incl column.
        Args:
            target_claims_data (Dataframe): Custom DataFrame with target claims data.
        Returns:
            data frame for joining by method read_data().
        """
        return (
            target_claims_data
            .select(TARGET_CLAIMS_DATA_COLS_MEMBER)
            .filter(F.col('member_level_incl') == INCLUDED)
            .drop('member_level_incl')
        )

    def read_data(self) -> None:
        """Method read_data reads parquet and csv data sources by FsDataReader,
            extracts required columns from data frame,
            filters the data before joining (e.g. based on underwriting date),
            transforms column names to lower case or even renames them,
            prepares data frame for final processing by method process().
        """
        if not self.interim_data:
            self.cust_info = self.prepare_cust_info(
                self.cust_info or read_cust_info()
            )

            """
            For target claims data we take only claims happened in 
            [MIN_EFF_DATE, MAX_EFF_DATE + PLAN_YEAR_DURATION] period. 
            This period is equivalent to [MIN_EFF_DATE, MAX_CLAIM_DATE] period.
            """
            self.target_claims_data = self.prepare_target_claims_data(
                self.target_claims_data or
                TargetClaimsProcessor(MIN_EFF_DATE, MAX_CLAIM_DATE).run()
            )

            self.interim_data = (
                self.target_claims_data
                    .join(self.cust_info, ['src_rpt_cust_id', 'effective_date'])
                    .drop('effective_date')
            )

    def process(self) -> DataFrame:
        """Method returns interim dataframe.
        Returns:
            processed data
        """
        return self.interim_data

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
            'hist_mths_avail': IntegerType(),
            'underwriting_date': TimestampType(),
            'quoted_members': IntegerType(),
            'quoted_pfm_rel': FloatType(),
            'train_subpartition': StringType(),
            'bus_segment_cd': StringType(),
            'comm_rt': IntegerType(),
            'lf_group': IntegerType(),
            'aso_group': IntegerType(),
            'mbr_mth_ag_cmpl_allow': FloatType()
        }
        return self.cast_types(df, schema_dict)
