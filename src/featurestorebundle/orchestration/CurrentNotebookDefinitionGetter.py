from box import Box
from pyspark.dbutils import DBUtils
from featurestorebundle.orchestration.NotebookDefinitionGetter import NotebookDefinitionGetter


class CurrentNotebookDefinitionGetter:
    def __init__(
        self,
        dbutils: DBUtils,
        notebook_definition_getter: NotebookDefinitionGetter,
    ):
        self.__dbutils = dbutils
        self.__notebook_definition_getter = notebook_definition_getter

    def get(self) -> Box:
        notebook_name = self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split("/")[-1]
        notebook_definition = self.__notebook_definition_getter.get(notebook_name)

        return notebook_definition
