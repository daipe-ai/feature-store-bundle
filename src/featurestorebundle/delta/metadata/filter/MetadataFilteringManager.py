from typing import List, Optional
from functools import reduce
from operator import __or__
from pyspark.sql import DataFrame
from pyspark.sql import functions as f


class MetadataFilteringManager:
    def filter(
        self,
        metadata: DataFrame,
        entity_name: Optional[str],
        features: Optional[List[str]],
        categories: Optional[List[str]],
        time_windows: Optional[List[str]],
        include_tags: Optional[List[str]],
        exclude_tags: Optional[List[str]],
    ) -> DataFrame:
        if entity_name is not None:
            metadata = self.__get_for_entity(metadata, entity_name)

        if features is not None:
            metadata = self.__get_for_features(metadata, features)

        if categories is not None:
            metadata = self.__get_for_categories(metadata, categories)

        if time_windows is not None:
            metadata = self.__get_for_time_windows(metadata, time_windows)

        if include_tags is not None:
            metadata = self.__include_tags(metadata, include_tags)

        if exclude_tags is not None:
            metadata = self.__exclude_tags(metadata, exclude_tags)

        return metadata

    def __get_for_entity(self, metadata: DataFrame, entity_name: str) -> DataFrame:
        return metadata.filter(f.col("entity") == entity_name)

    def __get_for_features(self, metadata: DataFrame, features: List[str]) -> DataFrame:
        return metadata.filter(f.col("feature").isin(features))

    def __get_for_categories(self, metadata: DataFrame, categories: List[str]) -> DataFrame:
        return metadata.filter(f.col("category").isin(categories))

    def __get_for_time_windows(self, metadata: DataFrame, time_windows: List[str]) -> DataFrame:
        return metadata.filter(f.col("extra").time_window.isin(time_windows))

    def __include_tags(self, metadata: DataFrame, tags: List[str]) -> DataFrame:
        return metadata.filter(reduce(__or__, (f.array_contains("tags", tag) for tag in tags)))

    def __exclude_tags(self, metadata: DataFrame, tags: List[str]) -> DataFrame:
        # pylint: disable=invalid-unary-operand-type
        return metadata.filter(~reduce(__or__, (f.array_contains("tags", tag) for tag in tags)))
