# pylint: disable=too-many-instance-attributes
class TableNames:
    def __init__(
        self,
        db_name: str,
        feature_table_name_template: str,
        feature_table_path_template: str,
        metadata_table_name_template: str,
        metadata_table_path_template: str,
        target_table_name_template: str,
        target_table_path_template: str,
        target_enum_table_name: str,
        target_enum_table_path: str,
    ):
        self.__db_name = db_name
        self.__feature_table_name_template = feature_table_name_template
        self.__feature_table_path_template = feature_table_path_template
        self.__metadata_table_name_template = metadata_table_name_template
        self.__metadata_table_path_template = metadata_table_path_template
        self.__target_table_name_template = target_table_name_template
        self.__target_table_path_template = target_table_path_template
        self.__target_enum_table_name = target_enum_table_name
        self.__target_enum_table_path = target_enum_table_path

    def get_db_name(self) -> str:
        return self.__db_name

    def get_features_table_name(self, entity_name) -> str:
        return self.__replace_placeholders(self.__feature_table_name_template, entity_name)

    def get_features_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name()}.{self.get_features_table_name(entity_name)}"

    def get_features_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__feature_table_path_template, entity_name)

    def get_metadata_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_table_name_template, entity_name)

    def get_metadata_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name()}.{self.get_metadata_table_name(entity_name)}"

    def get_metadata_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__metadata_table_path_template, entity_name)

    def get_targets_table_name(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_table_name_template, entity_name)

    def get_targets_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name()}.{self.get_targets_table_name(entity_name)}"

    def get_targets_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__target_table_path_template, entity_name)

    def get_targets_enum_table_name(self) -> str:
        return self.__target_enum_table_name

    def get_targets_enum_full_table_name(self) -> str:
        return f"{self.get_db_name()}.{self.get_targets_enum_table_name()}"

    def get_targets_enum_path(self) -> str:
        return self.__target_enum_table_path

    def __replace_placeholders(self, template: str, entity_name: str) -> str:
        return template.format(entity=entity_name)
