from box import Box
from featurestorebundle.orchestration.CurrentNotebookDefinitionGetter import CurrentNotebookDefinitionGetter


class DummyCurrentNotebookDefinitionGetter(CurrentNotebookDefinitionGetter):
    # pylint: disable=super-init-not-called
    def __init__(self):  # noqa
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
