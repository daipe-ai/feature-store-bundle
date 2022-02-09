import hashlib
from functools import reduce
from typing import List, Tuple
from logging import Logger
from pyspark.sql import DataFrame
from pyspark.sql import functions as f
from pyspark.sql.window import Window
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.schema import get_rainbow_table_hash_column, get_rainbow_table_features_column


class FeaturesPreparer:
    def __init__(
        self,
        logger: Logger,
        join_method: str,
        join_batch_size: int,
        checkpoint_after_join: bool,
        checkpoint_before_merge: bool,
    ):
        self.__logger = logger
        self.__join_method = join_method
        self.__join_batch_size = join_batch_size
        self.__checkpoint_after_join = checkpoint_after_join
        self.__checkpoint_before_merge = checkpoint_before_merge

    def prepare(self, features_storage: FeaturesStorage, feature_store: DataFrame, rainbow_table: DataFrame) -> Tuple[DataFrame, DataFrame]:
        entity = features_storage.entity
        feature_list = features_storage.feature_list

        base_dataframe = self.__prepare_base_dataframe(features_storage, feature_store, rainbow_table)

        if self.__checkpoint_before_merge:
            self.__logger.info("Checkpointing features data before merge")

            base_dataframe = base_dataframe.checkpoint()

            self.__logger.info("Checkpointing done")

        features_data = base_dataframe.select(
            entity.id_column,
            entity.time_column,
            get_rainbow_table_hash_column().name,
            *feature_list.get_names(),
        )

        rainbow_data = base_dataframe.select(
            get_rainbow_table_hash_column().name,
            get_rainbow_table_features_column().name,
        ).distinct()

        return features_data, rainbow_data

    def __prepare_base_dataframe(self, features_storage: FeaturesStorage, feature_store: DataFrame, rainbow_table: DataFrame) -> DataFrame:
        if not features_storage.results:
            raise Exception("There are no features to write.")

        entity = features_storage.entity
        feature_list = features_storage.feature_list
        results = features_storage.results
        pk_columns = [entity.id_column, entity.time_column]
        technical_columns = pk_columns + [get_rainbow_table_hash_column().name]

        registered_features = {col for col in feature_store.columns if col not in technical_columns}
        incoming_features = {*feature_list.get_names()}

        joined_results = self.__join_results(results, pk_columns)

        if registered_features == incoming_features:
            self.__logger.debug("Optimization: schema did not change")

            return self.__compute_new_hashes_optimized(joined_results, feature_list)

        joined_results = joined_results.withColumn("new_columns", f.array(*map(f.lit, feature_list.get_names())))

        return self.__compute_new_hashes(joined_results, feature_store, rainbow_table)

    def __compute_new_hashes(self, joined_results: DataFrame, feature_store: DataFrame, rainbow_table: DataFrame) -> DataFrame:  # noqa
        pk_columns = feature_store.columns[0:2]
        technical_columns = feature_store.columns[0:3]

        return (
            joined_results.join(feature_store.select(technical_columns), on=pk_columns, how="left")
            .join(rainbow_table, on=get_rainbow_table_hash_column().name, how="left")
            .withColumn("computed_columns", f.when(f.col("computed_columns").isNull(), f.array()).otherwise(f.col("computed_columns")))
            .withColumn("columns_union", f.array_sort(f.array_union("computed_columns", "new_columns")))
            .withColumn("new_features_hash", f.md5(f.concat_ws("`", "columns_union")))
            .drop("features_hash", "computed_columns")
            .withColumnRenamed("new_features_hash", "features_hash")
            .withColumnRenamed("columns_union", "computed_columns")
        )

    def __compute_new_hashes_optimized(self, joined_results: DataFrame, feature_list: FeatureList) -> DataFrame:  # noqa
        id_column = joined_results.columns[0]
        time_column = joined_results.columns[1]

        features_hash = hashlib.md5("`".join(sorted(feature_list.get_names())).encode()).hexdigest()

        return joined_results.select(
            id_column,
            time_column,
            f.lit(features_hash).alias(get_rainbow_table_hash_column().name),
            f.lit(f.array_sort(f.array(*map(f.lit, feature_list.get_names())))).alias(get_rainbow_table_features_column().name),
            *feature_list.get_names(),
        )

    def __join_results(self, results: List[DataFrame], pk_columns: List[str]):
        if self.__join_method == "left_with_checkpointing":
            join_method = self.__join_dataframes_using_left_join

        elif self.__join_method == "union_with_window":
            join_method = self.__join_dataframes_using_union_and_window

        else:
            raise Exception("Invalid join method")

        joined_results = join_method(results, pk_columns)

        if self.__checkpoint_after_join:
            self.__logger.info("Checkpointing features data after join")

            joined_results = joined_results.checkpoint()

            self.__logger.info("Checkpointing done")

        return joined_results

    def __join_dataframes_using_left_join(self, dfs: List[DataFrame], join_columns: List[str]) -> DataFrame:
        join_batch_counter = 0
        id_dataframes = [df.select(join_columns) for df in dfs]
        unique_ids_df = reduce(lambda df1, df2: df1.unionByName(df2), id_dataframes).distinct().cache()
        joined_df = unique_ids_df

        for df in dfs:
            join_batch_counter += 1
            joined_df = joined_df.join(df, on=join_columns, how="left")

            if join_batch_counter == self.__join_batch_size:
                joined_df = joined_df.checkpoint()
                join_batch_counter = 0

        return joined_df

    def __join_dataframes_using_union_and_window(self, dfs: List[DataFrame], join_columns: List[str]) -> DataFrame:  # noqa
        window = Window.partitionBy(*join_columns).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        union_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dfs)
        features = [col for col in union_df.columns if col not in join_columns]

        return (
            union_df.select(
                *join_columns,
                *[f.first(feature, ignorenulls=True).over(window).alias(feature) for feature in features],
            )
            .groupBy(join_columns)
            .agg(*[f.first(feature).alias(feature) for feature in features])
        )
