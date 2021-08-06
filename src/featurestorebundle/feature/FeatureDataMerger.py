from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as t
from delta.tables import DeltaTable
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from gql import gql, Client
from logging import Logger


class FeatureDataMerger:
    def __init__(self, metadata_api_enabled: bool, logger: Logger, gql_client: Client, spark: SparkSession):
        self.__metadata_api_enabled = metadata_api_enabled
        self.__logger = logger
        self.__gql_client = gql_client
        self.__spark = spark

    def merge(
        self,
        entity: Entity,
        feature_list: FeatureList,
        features_data: DataFrame,
        pk_columns: list,
        target_table_path: str,
        metadata_table_path: str,
    ):
        feature_names = feature_list.get_names()

        data_column_names = [
            field.name for field in features_data.schema.fields if field.name not in [entity.id_column, entity.time_column]
        ]

        if len(data_column_names) != len(feature_names):
            raise Exception(f"Number or dataframe columns ({len(data_column_names)}) != number of features defined ({len(feature_names)})")

        def build_update_set():
            update_set = {}

            for i, feature_name in enumerate(feature_names):
                update_set[feature_name] = f"source.{data_column_names[i]}"

            return update_set

        def build_insert_set():
            insert_set = build_update_set()
            insert_set[entity.id_column] = f"source.{entity.id_column}"
            insert_set[entity.time_column] = f"source.{entity.time_column}"

            return insert_set

        def build_merge_condition():
            conditions = []

            for pk in pk_columns:
                conditions.append(f"target.{pk} = source.{pk}")

            return " AND ".join(conditions)

        delta_table = DeltaTable.forPath(self.__spark, target_table_path)

        (
            delta_table.alias("target")
            .merge(features_data.alias("source"), build_merge_condition())
            .whenMatchedUpdate(set=build_update_set())
            .whenNotMatchedInsert(values=build_insert_set())
            .execute()
        )

        self.__write_metadata(metadata_table_path, feature_list)

    def __write_metadata(self, metadata_path: str, feature_list: FeatureList):
        metadata_columns = ["feature", "description", "category"]
        metadata = []

        for feature in feature_list.get_all():
            metadata.append([feature.name, feature.description, feature.category])

        metadata_df = self.__spark.createDataFrame(metadata, metadata_columns)

        if not DeltaTable.isDeltaTable(self.__spark, metadata_path):
            metadata_df.write.format("delta").save(metadata_path)
            return

        delta_table = DeltaTable.forPath(self.__spark, metadata_path)

        update_set = {
            "description": "source.description",
            "category": "source.category",
        }

        insert_set = {
            "feature": "source.feature",
            "description": "source.description",
            "category": "source.category",
        }

        (
            delta_table.alias("target")
            .merge(metadata_df.alias("source"), "target.feature = source.feature")
            .whenMatchedUpdate(set=update_set)
            .whenNotMatchedInsert(values=insert_set)
            .execute()
        )

    def __post_metadata_to_db(self, schema: t.StructType(), feature_list: FeatureList, entity: Entity):
        for field in schema[2:]:
            if field.name in feature_list.get_names():
                gql_query = gql(
                    f"""
                        mutation {{
                            createFeature(entity: "{entity.name}", name: "{field.name}", description: "{field.metadata.get('comment')}", category: "{field.metadata.get('category')}") {{
                                id,
                                existing,
                            }}
                        }}
                    """
                )

                try:
                    self.__gql_client.execute(gql_query)

                except BaseException:
                    self.__logger.warning("Cannot reach metadata api server. The metadata will not be written.")
