import ast
import pydoc
from pyspark.sql import DataFrame
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureList import FeatureList


class FeatureListFactory:
    def create(self, metadata: DataFrame) -> FeatureList:
        feature_instances = []
        rows = metadata.collect()

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
                row.start_date,
                row.frequency,
                row.last_compute_date,
                row.is_feature,
            )

            feature_instance = FeatureInstance(
                row.entity, row.feature, row.description, row.dtype, row.variable_type, row.extra, feature_template
            )
            feature_instances.append(feature_instance)

        return FeatureList(feature_instances)

    # pylint: disable=too-many-return-statements
    def __convert_fillna_value(self, fillna_value: str, fillna_value_type: str):
        type_ = pydoc.locate(fillna_value_type)

        if type_ is None:
            return None

        if type_ == str:
            return str(fillna_value)

        if type_ == int:
            return int(fillna_value)

        if type_ == float:
            return float(fillna_value)

        if type_ == bool:
            return bool(fillna_value)

        if type_ == list:
            return ast.literal_eval(fillna_value)

        if type_ == dict:
            return ast.literal_eval(fillna_value)

        raise Exception(f"fillna value '{fillna_value}' of type '{fillna_value_type}' cannot be converted")
