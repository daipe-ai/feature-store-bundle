from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureDataMerger import FeatureDataMerger
from featurestorebundle.feature.FeatureList import FeatureList
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
