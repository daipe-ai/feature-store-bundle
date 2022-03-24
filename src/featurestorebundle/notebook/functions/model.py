from typing import List, Optional

import mlflow
from pyspark.sql import types as t

from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface

from featurestorebundle.feature.FeaturesGetter import FeaturesGetter


def __load_spark_model(model_name: str):
    return mlflow.spark.load_model(f"models:/{model_name}/Production")


@input_decorator_function
def get_spark_model(model_name: str):
    def wrapper(_: ContainerInterface):
        return __load_spark_model(model_name)

    return wrapper


@input_decorator_function
def get_features_for_model(model_name: str, additional_columns: Optional[List[str]] = None, fillna_with=0):
    additional_columns = additional_columns or []

    def wrapper(container: ContainerInterface):
        features_getter: FeaturesGetter = container.get(FeaturesGetter)

        model = __load_spark_model(model_name)
        feature_names = list(set(model.stages[0].getInputCols() + additional_columns))
        features_df = features_getter.get_features(feature_names)

        subset = [
            feature.name
            for feature in features_df.schema.fields
            if isinstance(
                feature.dataType,
                (t.IntegerType, t.DoubleType, t.FloatType, t.DecimalType, t.LongType),
            )
        ]
        return features_df.fillna(fillna_with, subset=subset)

    return wrapper
