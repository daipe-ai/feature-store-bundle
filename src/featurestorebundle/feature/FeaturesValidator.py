from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.feature.schema import get_rainbow_table_hash_column


class FeaturesValidator:
    def validate(self, entity: Entity, features_data: DataFrame, feature_list: FeatureList):
        technical_cols = [entity.id_column, entity.time_column, get_rainbow_table_hash_column().name]

        data_column_names = list(
            map(
                lambda field: field.name,
                filter(lambda field: field.name not in technical_cols, features_data.schema.fields),
            )
        )

        feature_names = feature_list.get_names()

        if data_column_names != feature_names:
            raise Exception(f"Dataframe columns of size ({len(data_column_names)}) != features matched of size ({len(feature_names)})")
