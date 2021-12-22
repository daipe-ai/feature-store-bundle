from pyspark.sql import DataFrame
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage


class FeaturesPreparer:
    def prepare(self, features_storage: FeaturesStorage) -> DataFrame:
        join_batch_size = 10
        batch_counter = 0

        if not features_storage.results:
            raise Exception("There are no features to write.")

        pk_columns = [features_storage.entity.id_column, features_storage.entity.time_column]

        id_dataframes = [df.select(pk_columns) for df in features_storage.results]

        unique_ids_df = id_dataframes[0]

        for df in id_dataframes[1:]:
            unique_ids_df = unique_ids_df.unionByName(df)

        unique_ids_df = unique_ids_df.distinct()
        joined_df = unique_ids_df.cache()

        for df in features_storage.results:
            batch_counter += 1

            joined_df = joined_df.join(df, on=pk_columns, how="left")

            if batch_counter == join_batch_size:
                joined_df = joined_df.persist()
                batch_counter = 0

        return joined_df
