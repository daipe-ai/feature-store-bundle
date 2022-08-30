import os
from pyspark.dbutils import DBUtils
from databricksbundle.repos import repository_root_resolver


class NotebookMetadataGetter:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def get_absolute_path(self) -> str:
        return self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

    def get_relative_path(self) -> str:
        return os.path.relpath("/Workspace" + self.get_absolute_path(), repository_root_resolver.resolve())

    def get_name(self) -> str:
        return self.get_absolute_path().split("/")[-1]
