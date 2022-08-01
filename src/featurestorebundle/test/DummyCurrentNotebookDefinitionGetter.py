from box import Box
from featurestorebundle.orchestration.CurrentNotebookDefinitionGetter import CurrentNotebookDefinitionGetter


class DummyCurrentNotebookDefinitionGetter(CurrentNotebookDefinitionGetter):
    def __init__(self):  # noqa # pylint: disable=super-init-not-called
        pass

    def get(self) -> Box:
        return Box(
            {
                "notebook": "/foo/bar",
                "start_date": "2020-01-01",
                "frequency": "daily",
                "arguments": {},
            }
        )
