from typing import List
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql.window import Window
from pyspark.sql import functions as f
from featurestorebundle.delta.join.DataFrameJoinerInterface import DataFrameJoinerInterface


class UnionWithWindowJoiner(DataFrameJoinerInterface):
    def join(self, dataframes: List[DataFrame], join_columns: List[str]) -> DataFrame:
        window = Window.partitionBy(*join_columns).rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
        union_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), dataframes)
        columns = [col for col in union_df.columns if col not in join_columns]

        return (
            union_df.select(
                *join_columns,
                *[f.first(column, ignorenulls=True).over(window).alias(column) for column in columns],
            )
            .groupBy(join_columns)
            .agg(*[f.first(column).alias(column) for column in columns])
        )
