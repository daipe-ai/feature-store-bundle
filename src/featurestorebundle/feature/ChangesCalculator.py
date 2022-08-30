import itertools
from typing import List, Tuple

from pyspark.sql import Column
from pyspark.sql import functions as f

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList, MasterFeature
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate


class ChangesCalculator:
    def get_changes(self, master_features: List[MasterFeature], entity: Entity) -> Tuple[List[Column], FeatureList]:
        change_columns = []
        change_features = []

        for master_feature in master_features:
            combinations = list(itertools.combinations(master_feature.time_windows, 2))
            change_columns.extend([self.__change_column(master_feature.name, low, high) for low, high in combinations])
            change_features.extend([self.__change_feature(master_feature.features[0], lo, hi, entity.name) for lo, hi in combinations])

        return change_columns, FeatureList(entity, change_features)

    def __change_column(self, feature_name: str, low: str, high: str) -> Column:
        time_window_ratio = int(high[:-1]) / int(low[:-1])
        low_col = f.col(feature_name.format(time_window=low))
        high_col = f.col(feature_name.format(time_window=high))

        column_ratio = f.when((low_col == 0) & (high_col == 0), 0).otherwise(low_col / high_col)

        return (column_ratio * time_window_ratio).alias(feature_name.format(time_window=f"change_{low}_{high}"))

    def __change_feature(self, feature: FeatureInstance, low: str, high: str, entity_name: str) -> FeatureInstance:
        extra = feature.extra

        extra = {**extra, "time_window": f"change_{low}_{high}"}

        feature_name = feature.template.name_template.format(**extra)

        template = FeatureTemplate(
            name_template=feature.template.name_template,
            description_template=feature.template.description_template,
            fillna_value=0.0,
            fillna_value_type=float.__name__,
            location=feature.template.location,
            backend=feature.template.backend,
            notebook_name=feature.template.notebook_name,
            notebook_absolute_path=feature.template.notebook_absolute_path,
            notebook_relative_path=feature.template.notebook_relative_path,
            category=feature.template.category,
            owner=feature.template.owner,
            tags=feature.template.tags,
            start_date=feature.template.start_date,
            frequency=feature.template.frequency,
            last_compute_date=feature.template.last_compute_date,
        )

        return FeatureInstance.from_template(template, entity_name, feature_name, "double", "numerical", extra)
