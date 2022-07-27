from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList


class AddColumnsQueryBuilder:
    def build_add_columns_query(self, table_identifier: str, feature_list: FeatureList) -> str:
        add_column_sqls = [self.__build_add_column_query(feature) for feature in feature_list.get_all()]
        return f"ALTER TABLE {table_identifier} ADD COLUMNS ({','.join(add_column_sqls)})"

    def __build_add_column_query(self, feature: FeatureInstance) -> str:
        return f'`{feature.name}` {feature.storage_dtype} COMMENT "{feature.description}"'
