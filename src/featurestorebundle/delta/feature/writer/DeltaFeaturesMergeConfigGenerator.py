from featurestorebundle.delta.feature.writer.DeltaFeaturesMergeConfig import DeltaFeaturesMergeConfig
from featurestorebundle.entity.Entity import Entity
from pyspark.sql import DataFrame
from typing import List, Dict


class DeltaFeaturesMergeConfigGenerator:
    def generate(
        self,
        entity: Entity,
        features_data: DataFrame,
        pk_columns: List[str],
    ) -> DeltaFeaturesMergeConfig:
        technical_cols = [entity.id_column, entity.time_column]
        data_column_names = list(
            map(
                lambda field: field.name,
                filter(lambda field: field.name not in technical_cols, features_data.schema.fields),
            )
        )

        update_set = self.__build_set(data_column_names)
        insert_set = {**update_set, **self.__build_set(technical_cols)}
        merge_condition = " AND ".join(f"{self.__wrap_target(pk)} = {self.__wrap_source(pk)}" for pk in pk_columns)

        return DeltaFeaturesMergeConfig(
            source="source",
            target="target",
            data=features_data,
            merge_condition=merge_condition,
            update_set=update_set,
            insert_set=insert_set,
        )

    def __wrap_source(self, column: str) -> str:
        return self.__wrap("source", column)

    def __wrap_target(self, column: str) -> str:
        return self.__wrap("target", column)

    def __build_set(self, columns: List[str]) -> Dict[str, str]:
        return {column: self.__wrap_source(column) for column in columns}

    def __wrap(self, alias: str, column: str) -> str:  # noqa
        return f"{alias}.`{column}`"
