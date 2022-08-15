import ast
import pydoc
from pyspark.sql import DataFrame
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.delta.DataFrameSchemaMerger import DataFrameSchemaMerger
from featurestorebundle.delta.metadata.schema import get_metadata_schema


class FeatureListFactory:
    def __init__(self, dataframe_schema_merger: DataFrameSchemaMerger):
        self.__dataframe_schema_merger = dataframe_schema_merger

    def create(self, entity: Entity, metadata: DataFrame) -> FeatureList:
        rows = self.__merge_metadata_with_current_schema(metadata).collect()

        feature_instances = []

        for row in rows:
            feature_template = FeatureTemplate(
                row.feature_template,
                row.description_template,
                self.__convert_fillna_value(row.fillna_value, row.fillna_value_type),
                row.fillna_value_type,
                row.location,
                row.backend,
                row.notebook,
                row.category,
                row.owner,
                row.tags,
                row.start_date,
                row.frequency,
                row.last_compute_date,
            )

            feature_instance = FeatureInstance(
                row.entity, row.feature, row.description, row.dtype, row.variable_type, row.extra, feature_template
            )
            feature_instances.append(feature_instance)

        return FeatureList(entity, feature_instances)

    def __convert_fillna_value(self, fillna_value: str, fillna_value_type: str):
        type_ = pydoc.locate(fillna_value_type)

        if type_ is None:
            return None

        if type_ == str:
            return str(fillna_value)

        if type_ in [int, float, bool, list, dict]:
            return ast.literal_eval(fillna_value)

        raise Exception(f"fillna value '{fillna_value}' of type '{fillna_value_type}' cannot be converted")

    def __merge_metadata_with_current_schema(self, metadata: DataFrame) -> DataFrame:
        return self.__dataframe_schema_merger.merge(metadata, get_metadata_schema())
