import re
from featurestorebundle.databricks.DatabricksCurrentBranchResolver import DatabricksCurrentBranchResolver


# pylint: disable=too-many-instance-attributes
class TableNames:
    def __init__(
        self,
        feature_database_template: str,
        feature_table_template: str,
        feature_path_template: str,
        metadata_database_template: str,
        metadata_table_template: str,
        metadata_path_template: str,
        target_database_template: str,
        target_table_template: str,
        target_path_template: str,
        target_enum_database_template: str,
        target_enum_table_template: str,
        target_enum_path_template: str,
        current_branch_resolver: DatabricksCurrentBranchResolver,
    ):
        self.__feature_database_template = feature_database_template
        self.__feature_table_template = feature_table_template
        self.__feature_path_template = feature_path_template
        self.__metadata_database_template = metadata_database_template
        self.__metadata_table_template = metadata_table_template
        self.__metadata_path_template = metadata_path_template
        self.__target_database_template = target_database_template
        self.__target_table_template = target_table_template
        self.__target_path_template = target_path_template
        self.__target_enum_database_template = target_enum_database_template
        self.__target_enum_table_template = target_enum_table_template
        self.__target_enum_path_template = target_enum_path_template
        self.__current_branch_resolver = current_branch_resolver

    def get_features_database(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_database_template, entity_name)

    def get_features_table_name(self, entity_name) -> str:
        return self.__replace_placeholders(self.__feature_table_template, entity_name)

    def get_features_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_features_database(entity_name)}.{self.get_features_table_name(entity_name)}"

    def get_features_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_path_template, entity_name)

    def get_metadata_database(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_database_template, entity_name)

    def get_metadata_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_table_template, entity_name)

    def get_metadata_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_metadata_database(entity_name)}.{self.get_metadata_table_name(entity_name)}"

    def get_metadata_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_path_template, entity_name)

    def get_targets_database(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_database_template, entity_name)

    def get_targets_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_table_template, entity_name)

    def get_targets_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_targets_database(entity_name)}.{self.get_targets_table_name(entity_name)}"

    def get_targets_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_path_template, entity_name)

    def get_targets_enum_database(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_enum_database_template, entity_name)

    def get_targets_enum_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_enum_table_template, entity_name)

    def get_targets_enum_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_targets_enum_database(entity_name)}.{self.get_targets_enum_table_name(entity_name)}"

    def get_targets_enum_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_enum_path_template, entity_name)

    def __replace_placeholders(self, template: str, entity_name: str) -> str:
        replacements = {"entity": entity_name}

        if "{entity}" in template and entity_name is None:
            raise Exception("Cannot replace entity placeholder for 'None'")

        if "{current_branch}" in template:
            current_branch = self.__current_branch_resolver.resolve()
            current_branch = self.__convert_to_valid_identifier(current_branch)
            replacements["current_branch"] = current_branch

        return template.format(**replacements)

    def __convert_to_valid_identifier(self, identifier: str) -> str:
        return re.sub(r"[^0-9a-zA-Z]+", "_", identifier)
