from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureDataMerger import FeatureDataMerger
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.TablePreparer import TablePreparer
from featurestorebundle.feature.writer.FeaturesWriterInterface import FeaturesWriterInterface


class DeltaWriter(FeaturesWriterInterface):
    def __init__(self, table_preparer: TablePreparer, feature_data_merger: FeatureDataMerger):
        self.__table_preparer = table_preparer
        self.__feature_data_merger = feature_data_merger

    def write(self, result: DataFrame, entity: Entity, feature_list: FeatureList):
        self.__table_preparer.prepare(entity, feature_list)

        self.__feature_data_merger.merge(
            entity,
            feature_list,
            result,
        )

    def write_all(self, features_storage: FeaturesStorage):
        self.__table_preparer.prepare(features_storage.entity, features_storage.feature_list)

        joined_df = features_storage.results[0]

        for df in features_storage.results[1:]:
            joined_df = joined_df.join(df, on=[features_storage.entity.id_column, features_storage.entity.time_column], how="outer")

        self.__feature_data_merger.merge(
            features_storage.entity,
            features_storage.feature_list,
            joined_df,
        )
