import unittest
from pyfonycore.bootstrap import bootstrapped_container
from featurestorebundle.db.TableNames import TableNames

ENV = "test"


class TableNamesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.__container = bootstrapped_container.init("test")
        self.__table_names: TableNames = self.__container.get(TableNames)
        self.__entity = "test_entity"
        self.__target = "test_target"
        self.__timestamp = "2020-01-01"
        self.__hive_valid_timestamp = "20200101"

    def test_get_features_base_db_name(self):
        self.assertEqual(self.__table_names.get_features_base_db_name(self.__entity), f"{ENV}_feature_store")

    def test_get_metadata_base_db_name(self):
        self.assertEqual(self.__table_names.get_metadata_base_db_name(self.__entity), f"{ENV}_feature_store")

    def test_get_targets_base_db_name(self):
        self.assertEqual(self.__table_names.get_targets_base_db_name(self.__entity), f"{ENV}_target_store")

    def test_get_latest_features_db_name(self):
        self.assertEqual(self.__table_names.get_latest_features_db_name(self.__entity), f"{ENV}_feature_store")

    def test_get_latest_features_table_name(self):
        self.assertEqual(self.__table_names.get_latest_features_table_name(self.__entity), f"features_{self.__entity}")

    def test_get_latest_features_full_table_name(self):
        self.assertEqual(
            self.__table_names.get_latest_features_full_table_name(self.__entity), f"{ENV}_feature_store.features_{self.__entity}"
        )

    def test_get_latest_features_path(self):
        self.assertEqual(
            self.__table_names.get_latest_features_path(self.__entity), f"dbfs:/{ENV}_feature_store/features/{self.__entity}/latest.delta"
        )

    def test_get_target_features_db_name(self):
        self.assertEqual(self.__table_names.get_target_features_db_name(self.__entity), f"{ENV}_feature_store_target")

    def test_get_target_features_table_name(self):
        self.assertEqual(
            self.__table_names.get_target_features_table_name(self.__entity, self.__target),
            f"features_{self.__entity}_{self.__target}",
        )

    def test_get_target_features_full_table_name(self):
        self.assertEqual(
            self.__table_names.get_target_features_full_table_name(self.__entity, self.__target),
            f"{ENV}_feature_store_target.features_{self.__entity}_{self.__target}",
        )

    def test_get_target_features_path(self):
        self.assertEqual(
            self.__table_names.get_target_features_path(self.__entity, self.__target),
            f"dbfs:/{ENV}_feature_store/features/{self.__entity}/target/{self.__target}.delta",
        )

    def test_get_archive_features_db_name(self):
        self.assertEqual(self.__table_names.get_archive_features_db_name(self.__entity), f"{ENV}_feature_store_archive")

    def test_get_archive_features_table_name(self):
        self.assertEqual(
            self.__table_names.get_archive_features_table_name(self.__entity, self.__timestamp),
            f"features_{self.__entity}_{self.__hive_valid_timestamp}",
        )

    def test_get_archive_features_full_table_name(self):
        self.assertEqual(
            self.__table_names.get_archive_features_full_table_name(self.__entity, self.__timestamp),
            f"{ENV}_feature_store_archive.features_{self.__entity}_{self.__hive_valid_timestamp}",
        )

    def test_get_archive_features_path(self):
        self.assertEqual(
            self.__table_names.get_archive_features_path(self.__entity, self.__timestamp),
            f"dbfs:/{ENV}_feature_store/features/{self.__entity}/archive/{self.__hive_valid_timestamp}.delta",
        )


if __name__ == "__main__":
    unittest.main()
