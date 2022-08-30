from featurestorebundle.notebook.services.NotebookMetadataGetter import NotebookMetadataGetter


class DummyNotebookMetadataGetter(NotebookMetadataGetter):
    def get_name(self) -> str:
        return "notebook"

    def get_absolute_path(self) -> str:
        return "/Repos/repository/test_folder/notebook"

    def get_relative_path(self) -> str:
        return "test_folder/notebook"
