from pyspark.dbutils import DBUtils


class NotebookMetadataGetter:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def get_absolute_path(self) -> str:
        return self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    def get_name(self) -> str:
        return self.get_absolute_path().split("/")[-1]
