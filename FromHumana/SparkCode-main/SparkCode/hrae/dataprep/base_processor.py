"""Base class for all processors."""

from abc import ABC
from abc import abstractmethod
from typing import Dict

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DataType


class BaseProcessor(ABC):
    """BaseProcessor is parent class for all Data Processors for Rx and Med Claims,
       Customer and Member Data.
       Implements pattern Strategy. All methods except run() are abstract
       and will be implemented in the child classes.
       Method run() defines the logic and the order of processor method calls.
    """

    def cast_types(self,
                   df: DataFrame,
                   cast_dict: Dict[str, object]) -> DataFrame:
        """Casts column types.
        Args:
            df (DataFrame): Source DataFrame.
            cast_dict (Dict[str, object]): Dict with pairs in [col_name, new_type] format.
                new_type must be of type str or DataType
        Returns:
            Dataframe with typecasted columns.
        """
        cols = df.columns
        for col_name, new_type in cast_dict.items():
            if not isinstance(new_type, str) and not isinstance(new_type, DataType):
                raise TypeError(f'New type for column {col_name} must be str or DataType')
            if col_name not in cols:
                raise ValueError(f'There is no column {col_name} in dataframe')

        for col_name, new_type in cast_dict.items():
            df = df.withColumn(col_name, F.col(col_name).cast(new_type))

        return df

    def rename_cols(self,
                    df: DataFrame,
                    rename_dict: Dict[str, str]) -> DataFrame:
        """Renames multiple columns at a time.
        Args:
            df DataFrame Source DataFrame.
            rename_dict Dict[str, str] Dict with pairs in [old_name, new_name] format.
        Returns:
            Dataframe with renamed columns.
        """
        for col_name_old, col_name_new in rename_dict.items():
            df = df.withColumnRenamed(col_name_old, col_name_new)

        return df

    @abstractmethod
    def read_data(self) -> None:
        """Method read_data reads parquet data source from FS by FsDataReader,
           extracts required columns from data frame,
           filters the data before joining (e.g. based on underwriting date),
           transforms column names to lower case or even rename them,
           returns data frame for final processing (joining and aggregating) by method process().
        """

    @abstractmethod
    def process(self) -> DataFrame:
        """Implements all necessary transformations on source DataFrames
        Returns:
            processed data
        """

    @abstractmethod
    def ensure_schema(self, df: DataFrame) -> DataFrame:
        """Method ensure_schema transforms columns types of final dataframe from process() method.
        Returns:
            data with correct schema
        """

    def run(self) -> DataFrame:
        """Method run() comprises read_data(), process() and ensure_schema() methods.
        Returns:
            processed data
        """
        self.read_data()
        df = self.process()
        df = self.ensure_schema(df)
        return df
