from typing import List
from logging import Logger
from pyspark.sql import SparkSession
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.EntityGetter import EntityGetter
from featurestorebundle.metadata.deleter.MetadataDeleter import MetadataDeleter
from featurestorebundle.delta.feature.deleter.DeleteColumnsQueryBuilder import DeleteColumnsQueryBuilder
from featurestorebundle.feature.deleter.FeaturesDeleterInterface import FeaturesDeleterInterface


class DeltaPathFeaturesDeleter(FeaturesDeleterInterface):
    def __init__(
        self,
        logger: Logger,
        spark: SparkSession,
        table_names: TableNames,
        entity_getter: EntityGetter,
        metadata_deleter: MetadataDeleter,
        delete_columns_query_builder: DeleteColumnsQueryBuilder,
    ):
        self.__logger = logger
        self.__spark = spark
        self.__table_names = table_names
        self.__entity_getter = entity_getter
        self.__metadata_deleter = metadata_deleter
        self.__delete_columns_query_builder = delete_columns_query_builder

    def delete(self, features: List[str]):
        entity = self.__entity_getter.get()
        path = self.__table_names.get_features_path(entity.name)
        delete_query = self.__delete_columns_query_builder.build_delete_columns_query(f"delta.`{path}`", features)

        self.__logger.info(f"Deleting features {', '.join(features)}")

        self.__spark.sql(delete_query)

        self.__metadata_deleter.delete(features)

        self.__logger.info("Deleting features done")
