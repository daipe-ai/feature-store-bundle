class TableNames:
    def __init__(self, db_name_template: str, table_name_template: str, path_template: str):
        self.__db_name_template = db_name_template
        self.__table_name_template = table_name_template
        self.__path_template = path_template

    def get_full_tablename(self, entity_name: str) -> str:
        return f"{self.__db_name_template}.{self.__get_tablename(entity_name)}"

    def get_dbname(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__db_name_template, entity_name)

    def get_path(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__path_template, entity_name)

    def __get_tablename(self, entity_name: str) -> str:
        return self.__replace_placeholders(self.__table_name_template, entity_name)

    def __replace_placeholders(self, template: str, entity_name: str):
        return template.format(entity=entity_name)
