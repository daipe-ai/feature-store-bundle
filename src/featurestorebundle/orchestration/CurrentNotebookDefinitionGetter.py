from box import Box
from featurestorebundle.notebook.services.NotebookNameGetter import NotebookNameGetter
from featurestorebundle.orchestration.NotebookDefinitionGetter import NotebookDefinitionGetter


class CurrentNotebookDefinitionGetter:
    def __init__(
        self,
        notebook_name_getter: NotebookNameGetter,
        notebook_definition_getter: NotebookDefinitionGetter,
    ):
        self.__notebook_name_getter = notebook_name_getter
        self.__notebook_definition_getter = notebook_definition_getter

    def get(self) -> Box:
        notebook_name = self.__notebook_name_getter.get()
        notebook_definition = self.__notebook_definition_getter.get(notebook_name)

        return notebook_definition
