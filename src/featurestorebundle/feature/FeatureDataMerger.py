from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from featurestorebundle.db.TableNames import TableNames
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList


class FeatureDataMerger:
    def __init__(self, table_names: TableNames, spark: SparkSession):
        self.__table_names = table_names
        self.__spark = spark

    def merge(
        self,
        entity: Entity,
        feature_list: FeatureList,
        features_data: DataFrame,
        data_id_column: str,
        data_time_column: str,
    ):
        feature_names = feature_list.get_names()
        data_column_names = [field.name for field in features_data.schema.fields if field.name not in [data_id_column, data_time_column]]

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
                f"ON existing_data.{entity.id_column} = new_data.{data_id_column} "
                f"AND existing_data.{entity.time_column} = new_data.{data_time_column}\n"
                f"WHEN MATCHED THEN\n"
                f"{build_update_set_clause()}\n"
                f"WHEN NOT MATCHED\n"
                f"THEN INSERT ({entity.id_column}, {entity.time_column}, {','.join(feature_names)}) "
                f"VALUES ({data_id_column}, {data_time_column}, {build_insert_clause()})\n"
            )

        # store data to a view
        run_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        view_tablename = f"fv_{run_timestamp}"
        features_data.createOrReplaceTempView(view_tablename)
        merge_str = build_merge_into_string(entity, view_tablename)

        self.__spark.sql(merge_str)
