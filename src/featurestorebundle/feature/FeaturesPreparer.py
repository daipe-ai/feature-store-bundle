from functools import reduce
from typing import List
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity


class FeaturesPreparer:
    def __init__(self, join_batch_size):
        self.__join_batch_size = join_batch_size

    def prepare(self, entity: Entity, feature_dfs: List[DataFrame]) -> DataFrame:
        if not feature_dfs:
            raise Exception("There are no features to write.")

        batch_counter = 0

        pk_columns = [entity.id_column, entity.time_column]
        id_dataframes = [df.select(pk_columns) for df in feature_dfs]

        unique_ids_df = reduce(lambda df1, df2: df1.unionByName(df2), id_dataframes).distinct()

        joined_df = unique_ids_df.cache()

        for df in feature_dfs:
            batch_counter += 1

            joined_df = joined_df.join(df, on=pk_columns, how="left")

            if batch_counter == self.__join_batch_size:
                joined_df = joined_df.persist()
                batch_counter = 0

        return joined_df
