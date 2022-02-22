from typing import List
from pyspark.dbutils import DBUtils
from featurestorebundle.orchestration.NotebookTask import NotebookTask


class NotebookTasksFactory:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def create(self, notebooks: List[str]) -> List[NotebookTask]:
        parameters = dict(self.__dbutils.notebook.entry_point.getCurrentBindings())

        return [NotebookTask(notebook_path, parameters=parameters) for notebook_path in notebooks]
