from typing import List
from box import Box
from pyspark.dbutils import DBUtils
from featurestorebundle.orchestration.NotebookTask import NotebookTask


class NotebookTasksFactory:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def create(self, notebook_definitions: List[Box]) -> List[NotebookTask]:
        notebook_arguments = dict(self.__dbutils.notebook.entry_point.getCurrentBindings())
        notebook_tasks = []

        for notebook_definition in notebook_definitions:
            # for orchestration backwards compatibility
            if isinstance(notebook_definition, str):
                notebook_tasks.append(NotebookTask(notebook_definition, parameters=notebook_arguments))

            else:
                config_arguments = notebook_definition.arguments if "arguments" in notebook_definition else {}
                notebook_tasks.append(NotebookTask(notebook_definition.notebook, parameters={**notebook_arguments, **config_arguments}))

        return notebook_tasks
