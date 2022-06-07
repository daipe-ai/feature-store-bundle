from pyspark.sql import DataFrame
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.delta.join.DataFrameJoinerInterface import DataFrameJoinerInterface


class FeaturesJoiner:
    def __init__(self, dataframe_joiner: DataFrameJoinerInterface):
        self.__dataframe_joiner = dataframe_joiner

    def join(self, features_storage: FeaturesStorage) -> DataFrame:
        entity = features_storage.entity
        results = features_storage.results
        features_data = self.__dataframe_joiner.join(results, entity.get_primary_key())

        return features_data
