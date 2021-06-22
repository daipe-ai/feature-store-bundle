from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql import types as t
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

        def build_update_set_clause():
            def build_update_condition(feature_name, i):
                return f"existing_data.{feature_name} = new_data.`{data_column_names[i]}`"

            update_conditions = [build_update_condition(feature_name, i) for i, feature_name in enumerate(feature_names)]

            return f"UPDATE SET {','.join(update_conditions)}"

        def build_insert_clause():
            return ",".join([f"`{data_column_names[i]}`" for i, feature_name in enumerate(feature_names)])

        def build_merge_into_string(entity: Entity, view_tablename: str):
            return (
                f"MERGE INTO {self.__table_names.get_full_tablename(entity.name)} AS existing_data\n"
                f"USING {view_tablename} AS new_data\n"
                f"ON existing_data.{entity.id_column} = new_data.{entity.id_column} "
                f"AND existing_data.{entity.time_column} = new_data.{entity.time_column}\n"
                f"WHEN MATCHED THEN\n"
                f"{build_update_set_clause()}\n"
                f"WHEN NOT MATCHED\n"
                f"THEN INSERT ({entity.id_column}, {entity.time_column}, {','.join(feature_names)}) "
                f"VALUES ({entity.id_column}, {entity.time_column}, {build_insert_clause()})\n"
            )

        def add_metadata(feature_store_table_name: str, feature_list: FeatureList):
            df = self.__spark.read.table(feature_store_table_name)

            for field in df.schema:
                if field.name in feature_list.get_names():
                    feature = feature_list.get_feature_by_name(field.name)
                    metadata = field.metadata
                    metadata["comment"] = feature.description
                    metadata["category"] = feature.category
                    df = df.withColumn(feature.name, f.col(feature.name).alias("", metadata=metadata))

            df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(feature_store_table_name)

            if self.__metadata_api_enabled:
                self.__post_metadata_to_db(df.schema, feature_list, entity)

        # store data to a view
        run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        view_tablename = f"fv_{run_timestamp}"
        features_data.createOrReplaceTempView(view_tablename)
        merge_str = build_merge_into_string(entity, view_tablename)

        self.__spark.sql(merge_str)

        add_metadata(self.__table_names.get_full_tablename(entity.name), feature_list)

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
