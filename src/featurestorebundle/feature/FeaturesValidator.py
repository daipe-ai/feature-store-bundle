from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureList import FeatureList


class FeaturesValidator:
    def validate(self, entity: Entity, features_data: DataFrame, feature_list: FeatureList):
        technical_cols = [entity.id_column, entity.time_column]

        data_column_names = filter(lambda column: column not in technical_cols, features_data.columns)

        if len(set(features_data.columns)) != len(features_data.columns):
            raise Exception("Dataframe contains duplicate columns")

        if len(set(data_column_names)) != len(feature_list.get_names()):
            raise Exception("Dataframe columns do not match declared features")
