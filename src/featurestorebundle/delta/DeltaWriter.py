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

        join_batch_size = 10
        batch_counter = 0

        id_dataframes = [
            df.select(features_storage.entity.id_column, features_storage.entity.time_column) for df in features_storage.results
        ]

        unique_ids_df = id_dataframes[0]

        for df in id_dataframes[1:]:
            unique_ids_df.union(df)

        unique_ids_df = unique_ids_df.distinct()
        joined_df = unique_ids_df.cache()

        for df in features_storage.results:
            batch_counter += 1

            joined_df = joined_df.join(df, on=[features_storage.entity.id_column, features_storage.entity.time_column], how="left")

            if batch_counter == join_batch_size:
                joined_df = joined_df.persist()
                batch_counter = 0

        self.__feature_data_merger.merge(
            features_storage.entity,
            features_storage.feature_list,
            joined_df,
        )
