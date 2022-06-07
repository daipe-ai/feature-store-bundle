from pyspark.dbutils import DBUtils


class NotebookNameGetter:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def get(self) -> str:
        return self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
