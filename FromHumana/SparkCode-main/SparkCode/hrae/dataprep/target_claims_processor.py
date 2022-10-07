"""Module with `RxTargetProcessor` class and the constants necessary for its functioning."""
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, TimestampType, IntegerType, FloatType

# this imports should be commented if used in Databricks
from hrae.dataprep.med_target_processor import MedTargetProcessor
from hrae.dataprep.processor_constants import MIN_CLAIM_DATE, MAX_CLAIM_DATE
from hrae.dataprep.rx_target_processor import RxTargetProcessor
from hrae.dataprep.base_processor import BaseProcessor

# COMMAND ----------

# MAGIC %run ./base_processor

# COMMAND ----------

# MAGIC %run ./rx_target_processor

# COMMAND ----------

# MAGIC %run ./med_target_processor

# COMMAND ----------

# MAGIC %run ./processor_constants

# COMMAND ----------

spark = SparkSession.builder.getOrCreate()

KEY_COLS = [
    'src_rpt_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'effective_date'
]
"""List of columns used for DataFrame joining."""

PERIPHERAL_COLS = [
    'cov_months_medical',
    'hist_mths_avail',
    'total_members',
    'quoted_members',
    'quoted_pfm_rel',
    'member_level_incl',
    'group_level_incl',
]
"""List of columns which are not involved into the calculations
but must be present in the final output.
"""


class TargetClaimsProcessor(BaseProcessor):
    """Class for producing target variable (total agnostic allowed amount) for each member.

    `TargetClaimsProcessor` takes as inputs intermediate tables received from target processors
    for rx and med claims (`RxTargetProcessor`, `MedTargetProcessor`) and produces final output
    with total agnostic allowed amounts (target/response variable) and total allowed amounts
    calculated per member and uw dates.

    Final output will contain all members to-be included into final outputs on member
    (`member_level_incl = 1`) and group (`group_level_incl = 1`) levels.

    Both Member data and Group data processors will take output of `TargetClaimsProcessor`
    as input to calculate final target on the member and group levels, correspondingly.

    Attributes:
        min_date_lim (str): A date indicating minimal date starting from which
            claims are taken for calculation.
        max_date_lim (str): A date indicating maximal date starting from which
            claims are no more taken for calculation.
        rx_target_processor (RxTargetProcessor): Instance of `RxTargetProcessor` used to
            calculate the contribution of rx claims to the target variable.
        med_target_processor (MedTargetProcessor): Instance of `MedTargetProcessor` used to
            calculate the contribution of med claims to the target variable.
        rx_target_data (DataFrame): Output of `rx_target_processor`. Can not be initialized
            manually.
        med_target_data (DataFrame): Output of `med_target_processor`. Can not be initialized
            manually.

    Example:
        The simplest case is to create the instance of processor and call `run()` method.
        ```py
        df_target = TargetClaimsProcessor().run()
        ```

        Also it's possible to override default claim date period defined by date constants.
        ```py
        df_target = (
            TargetClaimsProcessor(min_date_lim='2010-10-01', max_date_lim='2020-12-31').run()
        )
        ```

        For unit-testing or some specific research it could be useful to create
        `TargetClaimsProcessor` based on the custom instances of rx and med target processors.
        Such a way you can use your custom DataFrames instead of reading data from FS.

        ```py
        # Here we create the instance of rx target processor
        # based on our custom DataFrame with rx claims data.
        rx_proc = RxTargetProcessor(rx_claims=custom_rx_df)

        # Med target processor will take all the data from the FS.
        med_proc = MedTargetProcessor()

        df_target = (
            TargetClaimsProcessor(
                rx_target_processor=rx_proc, med_target_processor=med_proc
            )
            .run()
        )
        ```
    """

    def __init__(self,
                 min_date_lim: str = MIN_CLAIM_DATE,
                 max_date_lim: str = MAX_CLAIM_DATE,
                 rx_target_processor: RxTargetProcessor = None,
                 med_target_processor: MedTargetProcessor = None):

        self.min_date_lim = min_date_lim
        self.max_date_lim = max_date_lim
        self.rx_target_processor = rx_target_processor
        self.med_target_processor = med_target_processor

    def form_peripheral_columns(self, df: DataFrame) -> DataFrame:
        """Produces a single column for columns with rx_ and med_ prefixes.

        In the output DataFrames received from `RxTargetProcessor` and `MedTargetProcessor`
        we have a group of columns which are not involved into the calculations but must be
        present in the final output of `TargetClaimsProcessor` (they are listed in the
        PERIPHERAL_COLS list variable).

        rx_ and med_ prefixes are added to these columns in order to resolve the ambiguity
        that occurs when med_target DataFrame is joined with the rx_target DataFrame. Such a
        way every peripheral column will have a pair with another prefix (for example
        rx_total_members <--> med_total_members).

        If member has both rx and med claims, values of columns in the every pair will be
        the same. We only need this duplication to handle cases when member doesn't
        have rx or med claims. In such cases one of the columns in pair will be empty
        after the full outer join.

        So we could just coalesce columns in the every pair and write result in the column with
        the corresponding name but without prefix. After that columns with prefixes are no more
        needed and could be dropped.

        Args:
            df (DataFrame): The source DataFrame.

        Returns:
            DataFrame with not null peripheral columns per each member and uw date.
        """

        for col_name in PERIPHERAL_COLS:
            df = (
                df.withColumn(
                    col_name,
                    F.coalesce(F.col('rx_' + col_name), F.col('med_' + col_name))
                )
                .drop('rx_' + col_name, 'med_' + col_name)
            )

        return df

    def sum_med_and_rx_total(self, df: DataFrame) -> DataFrame:
        """Adds columns with total allowed and total agnostic allowed amounts.

        Args:
            df (DataFrame): The source DataFrame.

        Returns:
            DataFrame with total columns.
        """

        return (
            df.withColumn(
                'total_ag_allow_amt',
                F.col('total_rx_ag_allow_amt') + F.col('total_med_ag_allow_amt')
            )
            .withColumn(
                'total_allow_amt',
                F.col('total_rx_allow_amt') + F.col('total_med_allow_amt')
            )
        )

    def pmpm_calc(self,
                  processed_col: str,
                  pmpm_col: str) -> Callable[[DataFrame], DataFrame]:
        """Implements pmpm (per member per month) calculation.

        Args:
            processed_col (str): The name of the column for which we want to calculate pmpm.
            pmpm_col (str): The name of the column with pmpm result.

        Returns:
            DataFrame with pmpm column calculated.
        """

        def _inner_func(df: DataFrame) -> DataFrame:
            return (
                df.withColumn(
                    pmpm_col,
                    F.col(processed_col) / F.col('cov_months_medical')
                )
            )

        return _inner_func

    def read_data(self) -> None:
        """Extracts rx and med target data.

        Note:
            This method itself doesn't trigger any real DataFrame transformations.
            It just initializes class fields for the further usage.
        """
        self.rx_target_processor = (
            self.rx_target_processor
            if self.rx_target_processor
            else RxTargetProcessor(self.min_date_lim, self.max_date_lim)
        )

        self.med_target_processor = (
            self.med_target_processor
            if self.med_target_processor
            else MedTargetProcessor(self.min_date_lim, self.max_date_lim)
        )

        self.rx_target_data = self.rx_target_processor.run()
        self.med_target_data = self.med_target_processor.run()

    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames.

        This method is created for orchestration purposes. Its main goal is to build complete
        end-to-end transformational flow starting from the intermediate tables received from
        the target processors for rx and med claims and resulting with with the final DataFrame
        with agnostic allowed amount pmpm - `mbr_mth_ag_cmpl_allow` (target column) and
        allowed amount pmpm - `mbr_mth_allow_icob_amt`.

        Returns:
            DataFrame with the target per member per month.
        """

        return (
            self.med_target_data
            .join(
                self.rx_target_data, on=KEY_COLS, how='full_outer'
            )
            .fillna(0, ['total_med_allow_amt',
                        'total_med_ag_allow_amt',
                        'total_rx_allow_amt',
                        'total_rx_ag_allow_amt'])
            .transform(self.form_peripheral_columns)
            .transform(self.sum_med_and_rx_total)
            .transform(self.pmpm_calc('total_ag_allow_amt', 'mbr_mth_ag_cmpl_allow'))
            .transform(self.pmpm_calc('total_allow_amt', 'mbr_mth_allow_icob_amt'))
            .drop(
                'total_med_allow_amt', 'total_med_ag_allow_amt',
                'total_rx_allow_amt', 'total_rx_ag_allow_amt',
                'total_allow_amt', 'total_ag_allow_amt'
            )
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
            'total_members': IntegerType(),
            'mbr_mth_ag_cmpl_allow': FloatType(),
            'mbr_mth_allow_icob_amt': FloatType()
        }
        return self.cast_types(df, schema_dict)
