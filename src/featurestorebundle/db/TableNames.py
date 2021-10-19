class TableNames:
    def __init__(
        self,
        db_name_template: str,
        latest_table_name_template: str,
        historized_table_name_template: str,
        path_template: str,
        historized_path_template: str,
        archive_path_template: str,
    ):
        self.__db_name_template = db_name_template
        self.__latest_table_name_template = latest_table_name_template
        self.__historized_table_name_template = historized_table_name_template
        self.__latest_path_template = path_template
        self.__historized_path_template = historized_path_template
        self.__archive_path_template = archive_path_template

    def get_db_name(self, entity_name):
        return self.__replace_placeholders(self.__db_name_template, entity_name)

    def get_latest_table_name(self, entity_name):
        return self.__replace_placeholders(self.__latest_table_name_template, entity_name)

    def get_historized_table_name(self, entity_name):
        return self.__replace_placeholders(self.__historized_table_name_template, entity_name)

    def get_latest_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name(entity_name)}.{self.get_latest_table_name(entity_name)}"

    def get_historized_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name(entity_name)}.{self.get_historized_table_name(entity_name)}"

    def get_latest_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__latest_path_template, entity_name)

    def get_historized_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__historized_path_template, entity_name)

    def get_latest_metadata_table(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__latest_table_name_template.replace("{entity}", "{entity}_metadata"), entity_name)

    def get_historized_metadata_table(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__historized_table_name_template.replace("{entity}", "{entity}_metadata"), entity_name)

    def get_latest_metadata_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name(entity_name)}.{self.get_latest_metadata_table(entity_name)}"

    def get_historized_metadata_full_table_name(self, entity_name: str) -> str:
        return f"{self.get_db_name(entity_name)}.{self.get_historized_metadata_table(entity_name)}"

    def get_latest_metadata_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__latest_path_template.replace("{entity}", "{entity}_metadata"), entity_name)

    def get_historized_metadata_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__historized_path_template.replace("{entity}", "{entity}_metadata"), entity_name)

    def get_archive_path(self, entity_name: str, date: str) -> str:
        return self.__archive_path_template.format(entity=entity_name, date=date)

    def __replace_placeholders(self, template: str, entity_name: str):
        return template.format(entity=entity_name)
