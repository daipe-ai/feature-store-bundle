import re
from typing import Dict
from box import Box
from featurestorebundle.databricks.repos.DatabricksCurrentBranchResolver import DatabricksCurrentBranchResolver


# pylint: disable=too-many-public-methods
class TableNames:
    def __init__(
        self,
        feature_tables: Box,
        metadata_tables: Box,
        target_tables: Box,
        current_branch_resolver: DatabricksCurrentBranchResolver,
    ):
        self.__feature_tables = feature_tables
        self.__metadata_tables = metadata_tables
        self.__target_tables = target_tables
        self.__current_branch_resolver = current_branch_resolver

    def get_features_base_db_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_tables.base_db, self.__get_base_replacements(entity_name))

    def get_metadata_base_db_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_tables.base_db, self.__get_base_replacements(entity_name))

    def get_targets_base_db_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.base_db, self.__get_base_replacements(entity_name))

    def get_latest_features_db_name(self, entity_name: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "database": self.__feature_tables.base_db}

        return self.__replace_placeholders(self.__feature_tables.latest_table.db_template, replacements)

    def get_latest_features_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_tables.latest_table.table_template, self.__get_base_replacements(entity_name))

    def get_latest_features_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_latest_features_db_name(entity_name)}.{self.get_latest_features_table_name(entity_name)}"

    def get_latest_features_full_table_name_for_base_db(self, db_name: str, entity_name: str):
        replacements = {**self.__get_base_replacements(entity_name), "database": db_name}

        return (
            f"{self.__replace_placeholders(self.__feature_tables.latest_table.db_template, replacements)}."
            f"{self.get_latest_features_table_name(entity_name)}"
        )

    def get_latest_features_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_tables.latest_table.path_template, self.__get_base_replacements(entity_name))

    def get_archive_features_db_name(self, entity_name: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "database": self.__feature_tables.base_db}

        return self.__replace_placeholders(self.__feature_tables.archive_table.db_template, replacements)

    def get_archive_features_table_name(self, entity_name: str, timestamp: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "timestamp": timestamp}

        return self.__replace_placeholders(self.__feature_tables.archive_table.table_template, replacements)

    def get_archive_features_full_table_name(self, entity_name: str, timestamp: str) -> str:
        return f"{self.get_archive_features_db_name(entity_name)}.{self.get_archive_features_table_name(entity_name, timestamp)}"

    def get_archive_features_full_table_name_for_base_db(self, db_name: str, entity_name: str, timestamp: str):
        replacements = {**self.__get_base_replacements(entity_name), "database": db_name}

        return (
            f"{self.__replace_placeholders(self.__feature_tables.archive_table.db_template, replacements)}."
            f"{self.get_archive_features_table_name(entity_name, timestamp)}"
        )

    def get_archive_features_path(self, entity_name: str, timestamp: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "timestamp": timestamp}

        return self.__replace_placeholders(self.__feature_tables.archive_table.path_template, replacements)

    def get_target_features_db_name(self, entity_name: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "database": self.__feature_tables.base_db}

        return self.__replace_placeholders(self.__feature_tables.target_table.db_template, replacements)

    def get_target_features_table_name(self, entity_name: str, target: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "target": target}

        return self.__replace_placeholders(self.__feature_tables.target_table.table_template, replacements)

    def get_target_features_full_table_name(self, entity_name: str, target: str) -> str:
        return f"{self.get_target_features_db_name(entity_name)}.{self.get_target_features_table_name(entity_name, target)}"

    def get_target_features_full_table_name_for_base_db(self, db_name: str, entity_name: str, target: str):
        replacements = {**self.__get_base_replacements(entity_name), "database": db_name}

        return (
            f"{self.__replace_placeholders(self.__feature_tables.target_table.db_template, replacements)}."
            f"{self.get_target_features_table_name(entity_name, target)}"
        )

    def get_target_features_path(self, entity_name: str, target: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "target": target}

        return self.__replace_placeholders(self.__feature_tables.target_table.path_template, replacements)

    def get_latest_metadata_db_name(self, entity_name: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "database": self.__metadata_tables.base_db}

        return self.__replace_placeholders(self.__metadata_tables.latest_table.db_template, replacements)

    def get_latest_metadata_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_tables.latest_table.table_template, self.__get_base_replacements(entity_name))

    def get_latest_metadata_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_latest_metadata_db_name(entity_name)}.{self.get_latest_metadata_table_name(entity_name)}"

    def get_latest_metadata_full_table_name_for_base_db(self, db_name: str, entity_name: str):
        replacements = {**self.__get_base_replacements(entity_name), "database": db_name}

        return (
            f"{self.__replace_placeholders(self.__metadata_tables.latest_table.db_template, replacements)}."
            f"{self.get_latest_metadata_table_name(entity_name)}"
        )

    def get_latest_metadata_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_tables.latest_table.path_template, self.__get_base_replacements(entity_name))

    def get_target_metadata_db_name(self, entity_name: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "database": self.__metadata_tables.base_db}

        return self.__replace_placeholders(self.__metadata_tables.target_table.db_template, replacements)

    def get_target_metadata_table_name(self, entity_name: str, target: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "target": target}

        return self.__replace_placeholders(self.__metadata_tables.target_table.table_template, replacements)

    def get_target_metadata_full_table_name(self, entity_name: str, target: str) -> str:
        return f"{self.get_target_metadata_db_name(entity_name)}.{self.get_target_metadata_table_name(entity_name, target)}"

    def get_target_metadata_full_table_name_for_base_db(self, db_name: str, entity_name: str, target: str):
        replacements = {**self.__get_base_replacements(entity_name), "database": db_name}

        return (
            f"{self.__replace_placeholders(self.__metadata_tables.target_table.db_template, replacements)}."
            f"{self.get_target_metadata_table_name(entity_name, target)}"
        )

    def get_target_metadata_path(self, entity_name: str, target: str) -> str:
        replacements = {**self.__get_base_replacements(entity_name), "target": target}

        return self.__replace_placeholders(self.__metadata_tables.target_table.path_template, replacements)

    def get_targets_db_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.table.db_template, self.__get_base_replacements(entity_name))

    def get_targets_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.table.table_template, self.__get_base_replacements(entity_name))

    def get_targets_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_targets_db_name(entity_name)}.{self.get_targets_table_name(entity_name)}"

    def get_targets_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.table.path_template, self.__get_base_replacements(entity_name))

    def get_targets_enum_db_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.enum_table.db_template, self.__get_base_replacements(entity_name))

    def get_targets_enum_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.enum_table.table_template, self.__get_base_replacements(entity_name))

    def get_targets_enum_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_targets_enum_db_name(entity_name)}.{self.get_targets_enum_table_name(entity_name)}"

    def get_targets_enum_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_tables.enum_table.path_template, self.__get_base_replacements(entity_name))

    def __replace_placeholders(self, template: str, replacements: Dict[str, str]) -> str:
        if "{entity}" in template and ("entity" not in replacements or replacements["entity"] is None):
            raise Exception("Cannot replace entity placeholder, try to pass entity name as parameter")

        if "{current_branch}" in template:
            current_branch = self.__current_branch_resolver.resolve()
            current_branch = self.__convert_to_valid_identifier(current_branch)

            replacements["current_branch"] = current_branch

        if "timestamp" in replacements:
            timestamp = self.__convert_to_valid_identifier(replacements["timestamp"])

            replacements["timestamp"] = timestamp

        return template.format(**replacements)

    def __get_base_replacements(self, entity_name: str) -> Dict[str, str]:
        return {"entity": entity_name}

    def __convert_to_valid_identifier(self, identifier: str) -> str:
        return re.sub(r"[^0-9a-zA-Z]+", "", identifier)
