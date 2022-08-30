from box import Box
from featurestorebundle.notebook.services.NotebookMetadataGetter import NotebookMetadataGetter
from featurestorebundle.orchestration.NotebookDefinitionGetter import NotebookDefinitionGetter


class CurrentNotebookDefinitionGetter:
    def __init__(
        self,
        notebook_metadata_getter: NotebookMetadataGetter,
        notebook_definition_getter: NotebookDefinitionGetter,
    ):
        self.__notebook_metadata_getter = notebook_metadata_getter
        self.__notebook_definition_getter = notebook_definition_getter

    def get(self) -> Box:
        notebook_name = self.__notebook_metadata_getter.get_name()
        notebook_definition = self.__notebook_definition_getter.get(notebook_name)

        return notebook_definition
