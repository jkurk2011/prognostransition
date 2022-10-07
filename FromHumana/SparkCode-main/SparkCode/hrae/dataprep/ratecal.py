from typing import Callable, List, Tuple

import pyspark.sql.functions as F
import pyspark.sql.types as T
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
from pyspark.sql import DataFrame, SparkSession

spark = SparkSession.builder.getOrCreate()

RATE_CALIBRATED_COLS = [
    'src_rpt_cust_id',
    'sdr_person_id',
    'underwriting_date',
    'rate_calibrated'
]


class Ratecal:
    """Rate calibration model that predicts 'rate_calibrated' for member data base on
    'comm_rt', 'lf_group', 'aso_group', and 'bus_segment'.
    """
    def __init__(self, target):
        self.target = target

    @staticmethod
    def split_bus_segment(mbr_data: DataFrame) -> DataFrame:
        """Function that performs OHE for 'bus_segment_cd' column.
        Args:
            mbr_data DataFrame with member data
        Returns:
            df after all transformations.
        """
        # for ratecal model fitting, data of customers from unknown segment ("U") should be excluded
        mbr_data = mbr_data[mbr_data['bus_segment_cd'] != 'U']
        seg_types = mbr_data.select('bus_segment_cd').distinct().rdd.flatMap(lambda x: x).collect()
        seg_dummies = [F.when(F.col('bus_segment_cd') == seg_type, 1).otherwise(0).alias('bus_segment_' + seg_type)
                       for seg_type in seg_types]
        mbr_data = mbr_data.select(*mbr_data.columns, *seg_dummies).drop('bus_segment_cd')
        return mbr_data

    @staticmethod
    def get_feature_cols(train_data: DataFrame) -> List:
        """Method that creates a list of columns that would be used as features in model.
        Because 'bus_segment_cd' can contain different values, feature columns shouldn't be a constant.
        Args:
            train_data: df that would be used for training
        Returns:
            list of features
        """
        feature_columns = [x for x in train_data.columns if
                           (x in ['comm_rt', 'lf_group', 'aso_group'] or x.startswith('bus_segment_'))]
        return feature_columns

    def prepare_model(self, train_data: DataFrame, feature_columns: List):
        """Method that prepares model, vector assembler, train and test sets.
        Args:
            train_data: df with training data
            feature_columns: list of features
        Returns:
            vector assembler, regressor, train and test sets
        """
        vec_assembler = VectorAssembler(inputCols=feature_columns,
                                        outputCol='features')
        regressor = LinearRegression(featuresCol='features',
                                     labelCol=self.target,
                                     predictionCol='rate_calibrated')

        train = train_data[train_data['train_subpartition'] == 'train']
        test = train_data[train_data['train_subpartition'] == 'valid']

        return vec_assembler, regressor, train, test

    def prepare_data(self, data: DataFrame) -> Tuple[DataFrame, List]:
        """Method that prepares data by splitting 'bus_segment_cd',
        and then prepares features list from a resulted DF.
        Then all feature columns are casted to integer, and null values are filled with 0s.
        Args:
            data: raw DataFrame created as an output of MemberDataProcessor
        Returns:
            DataFrame with splitted 'bus_segment_cd' column, with feature columns cast to integer and without nulls.
            List of feature columns
        """
        mbr_data = self.split_bus_segment(data)
        feature_columns = self.get_feature_cols(mbr_data)
        for col in feature_columns:
            mbr_data = mbr_data.withColumn(col, mbr_data[col].cast(T.IntegerType()))
        mbr_data = mbr_data.fillna(0)
        return mbr_data, feature_columns


def add_rate_calibrated(mbr_interim_data: DataFrame, ratecal_interim_df: DataFrame) -> DataFrame:
    """Function add_rate_calibrated merges two dfs to create final DataFrame with all required columns.
    All calculation rules can be found in
    [wiki](https://dev.azure.com/humana/Digital%20Health%20and%20Analytics/_wiki/wikis/Digital-Health-and-Analytics.wiki/6777/5.-Member-Data-Processor)
    Args:
        mbr_interim_data: DataFrame that is an output of MemberDataProcessor
        ratecal_interim_df: DataFrame with rate_calibrated column, output of Ratecal model
    Returns:
        data frame with column rate_calibrated.
    """

    return (
        mbr_interim_data
            .join(ratecal_interim_df.select(RATE_CALIBRATED_COLS),
                  on=['src_rpt_cust_id', 'sdr_person_id', 'underwriting_date'], how='left')
            .fillna(0, subset=['rate_calibrated'])
    )
