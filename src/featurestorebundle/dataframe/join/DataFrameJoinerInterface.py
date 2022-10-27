from abc import ABC, abstractmethod
from typing import List
from pyspark.sql import DataFrame


class DataFrameJoinerInterface(ABC):
    @abstractmethod
    def join(self, dataframes: List[DataFrame], join_columns: List[str]) -> DataFrame:
        pass
