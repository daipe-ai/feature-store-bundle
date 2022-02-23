import os
import uuid
import tempfile
import pickle
from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.entity.EntityGetter import EntityGetter


class Serializator:
    def __init__(self, spark: SparkSession, entity_getter: EntityGetter):
        self.__spark = spark
        self.__entity_getter = entity_getter

    def serialize(self, result: DataFrame, feature_list: FeatureList, orchestration_id: str):
        unique_id = uuid.uuid4().hex
        result_temp_view_name = f"result_{orchestration_id}_{unique_id}"
        feature_list_path = f"{tempfile.gettempdir()}/feature_list_{orchestration_id}_{unique_id}"

        result.createOrReplaceGlobalTempView(result_temp_view_name)  # pyre-ignore[29]

        with open(feature_list_path, "wb") as f:
            pickle.dump(feature_list, f)

    def deserialize(self, orchestration_id: str) -> FeaturesStorage:
        results_and_feature_lists = self.__get_results_and_feature_lists(orchestration_id)
        entity = self.__entity_getter.get()
        features_storage = FeaturesStorage(entity)

        for result, feature_list in results_and_feature_lists:
            features_storage.add(result, feature_list)

        return features_storage

    def __get_results_and_feature_lists(self, orchestration_id: str) -> List[Tuple[DataFrame, FeatureList]]:
        temp_views = sorted(self.__get_result_temp_views(orchestration_id))
        feature_list_paths = sorted(self.__get_feature_list_paths(orchestration_id))

        if len(temp_views) == 0 or len(feature_list_paths) == 0:
            raise Exception("There are no features to write")

        if len(temp_views) != len(feature_list_paths):
            raise Exception("Error reconstructing features storage, number of results != number of feature lists")

        return [
            (self.__deserialize_result(temp_view), self.__deserialize_feature_list(feature_list_path))
            for temp_view, feature_list_path in zip(temp_views, feature_list_paths)
        ]

    def __deserialize_result(self, temp_view: str) -> DataFrame:
        return self.__spark.read.table(f"global_temp.{temp_view}")

    def __deserialize_feature_list(self, pickle_path: str) -> FeatureList:
        with open(pickle_path, "rb") as f:
            return pickle.load(f)

    def __get_result_temp_views(self, orchestration_id: str) -> List[str]:
        return [
            table.name for table in self.__spark.catalog.listTables("global_temp") if table.name.startswith(f"result_{orchestration_id}")
        ]

    def __get_feature_list_paths(self, orchestration_id) -> List[str]:
        return [
            f"{tempfile.gettempdir()}/{file}"
            for file in os.listdir(tempfile.gettempdir())
            if file.startswith(f"feature_list_{orchestration_id}")
        ]
