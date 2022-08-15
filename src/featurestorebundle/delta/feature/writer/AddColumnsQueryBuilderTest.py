import unittest
import datetime as dt
from pyspark.sql import types as t
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeatureTemplate import FeatureTemplate
from featurestorebundle.feature.FeatureInstance import FeatureInstance
from featurestorebundle.feature.FeatureList import FeatureList

from featurestorebundle.delta.feature.writer.AddColumnsQueryBuilder import AddColumnsQueryBuilder


class AddColumnQueryBuilderTest(unittest.TestCase):
    def test_sql_query_builder_build_add_columns_string(self):
        add_columns_query_builder = AddColumnsQueryBuilder()

        entity = Entity(
            name="test_entity",
            id_column="id",
            id_column_type=t.StringType(),
            time_column="timestamp",
            time_column_type=t.TimestampType(),
        )
        feature_template = FeatureTemplate(
            name_template="test_name",
            description_template="test_description",
            fillna_value="",
            fillna_value_type="string",
            location="datalake/path",
            backend="delta_table",
            notebook="test_notebook",
            category="test_category",
            owner="test_owner",
            tags=["feature"],
            start_date=dt.datetime(2020, 1, 1),
            frequency="daily",
            last_compute_date=dt.datetime(2020, 1, 1),
        )
        feature_instance = FeatureInstance(
            entity="test_entity",
            name="test_name",
            description="Test description.",
            dtype="string",
            variable_type="string",
            extra={},
            template=feature_template,
        )
        feature_list = FeatureList(entity, [feature_instance])

        add_columns_query = add_columns_query_builder.build_add_columns_query(
            table_identifier="test_database.test_table", feature_list=feature_list
        )
        add_columns_query_expected = 'ALTER TABLE test_database.test_table ADD COLUMNS (`test_name` string COMMENT "Test description.")'

        self.assertEqual(add_columns_query_expected, add_columns_query)


if __name__ == "__main__":
    unittest.main()
