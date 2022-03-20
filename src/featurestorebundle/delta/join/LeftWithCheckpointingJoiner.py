from typing import List
from functools import reduce
from pyspark.sql import DataFrame
from featurestorebundle.checkpoint.CheckpointDirSetter import CheckpointDirSetter
from featurestorebundle.delta.join.DataFrameJoinerInterface import DataFrameJoinerInterface


class LeftWithCheckpointingJoiner(DataFrameJoinerInterface):
    def __init__(
        self,
        join_batch_size: int,
        checkpoint_dir_setter: CheckpointDirSetter,
    ):
        self.__join_batch_size = join_batch_size
        self.__checkpoint_dir_setter = checkpoint_dir_setter

    def join(self, dataframes: List[DataFrame], join_columns: List[str]) -> DataFrame:
        join_batch_counter = 0
        id_dataframes = [df.select(join_columns) for df in dataframes]
        unique_ids_df = reduce(lambda df1, df2: df1.unionByName(df2), id_dataframes).distinct().cache()
        joined_df = unique_ids_df

        self.__checkpoint_dir_setter.set_checkpoint_dir_if_necessary()

        for df in dataframes:
            join_batch_counter += 1
            joined_df = joined_df.join(df, on=join_columns, how="left")

            if join_batch_counter == self.__join_batch_size:
                joined_df = joined_df.checkpoint()
                join_batch_counter = 0

        return joined_df
