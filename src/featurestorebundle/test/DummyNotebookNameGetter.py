from featurestorebundle.notebook.services.NotebookNameGetter import NotebookNameGetter


class DummyNotebookNameGetter(NotebookNameGetter):
    def get(self) -> str:
        return "notebook"
