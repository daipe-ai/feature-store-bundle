from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
from delta.tables import DeltaTable
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from gql import gql, Client
from logging import Logger


class FeatureDataMerger:
    def __init__(self, metadata_api_enabled: bool, logger: Logger, gql_client: Client, table_names: TableNames, spark: SparkSession):
        self.__metadata_api_enabled = metadata_api_enabled
        self.__logger = logger
        self.__gql_client = gql_client
        self.__table_names = table_names
        self.__spark = spark

    def merge(
        self,
        entity: Entity,
        feature_list: FeatureList,
        features_data: DataFrame,
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

        def build_pruning_condition():
            rows = features_data.select(entity.time_column).distinct().collect()
            time_records = [getattr(row, entity.time_column) for row in rows]
            run_dates = [f"DATE'{d.year}-{d.month}-{d.day}'" for d in time_records]

            return f"target.{entity.time_column} IN ({', '.join(run_dates)})"

        def build_merge_condition():
            merge_condition = f"""
                {build_pruning_condition()} AND
                target.{entity.id_column} = source.{entity.id_column} AND
                target.{entity.time_column} = source.{entity.time_column}
            """

            return merge_condition

        def add_metadata(feature_store_table_name: str, feature_list: FeatureList):
            df = self.__spark.read.table(feature_store_table_name)

            for field in df.schema:
                if field.name in feature_list.get_names():
                    feature = feature_list.get_feature_by_name(field.name)
                    metadata = field.metadata
                    metadata["comment"] = feature.description
                    metadata["category"] = feature.category
                    df = df.withColumn(feature.name, f.col(feature.name).alias("", metadata=metadata))

            df.write.partitionBy(entity.time_column).format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
                feature_store_table_name
            )

            if self.__metadata_api_enabled:
                self.__post_metadata_to_db(df.schema, feature_list, entity)

        delta_table = DeltaTable.forName(self.__spark, self.__table_names.get_full_tablename(entity.name))

        delta_table.alias("target").merge(features_data.alias("source"), build_merge_condition()).whenMatchedUpdate(
            set=build_update_set()
        ).whenNotMatchedInsert(values=build_insert_set()).execute()

        # add_metadata(self.__table_names.get_full_tablename(entity.name), feature_list)

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
