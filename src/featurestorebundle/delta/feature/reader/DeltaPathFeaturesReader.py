from logging import Logger
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.delta.PathExistenceChecker import PathExistenceChecker
from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.feature.reader.FeaturesReaderInterface import FeaturesReaderInterface
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema


class DeltaPathFeaturesReader(FeaturesReaderInterface):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        path_existence_checker: PathExistenceChecker,
        empty_dataframe_creator: EmptyDataFrameCreator,
        entity_getter: EntityGetter,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__path_existence_checker = path_existence_checker
        self.__empty_dataframe_creator = empty_dataframe_creator
        self.__entity_getter = entity_getter

    def read(self, entity_name: str) -> DataFrame:
        path = self.__table_names.get_features_path(entity_name)

        self.__logger.info(f"Reading features from path {path}")

        return self.__spark.read.format("delta").load(path)

    def read_safe(self, entity_name: str) -> DataFrame:
        entity = self.__entity_getter.get_by_name(entity_name)
        path = self.__table_names.get_features_path(entity_name)

        if not self.exists(entity_name):
            return self.__empty_dataframe_creator.create(get_feature_store_initial_schema(entity))

        return self.__spark.read.format("delta").load(path)

    def exists(self, entity_name: str) -> bool:
        path = self.__table_names.get_features_path(entity_name)

        return self.__path_existence_checker.exists(path)
