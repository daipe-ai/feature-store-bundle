from featurestorebundle.databricks.DatabricksCurrentBranchResolver import DatabricksCurrentBranchResolver


class DummyDatabricksCurrentBranchResolver(DatabricksCurrentBranchResolver):
    def __init__(self):  # noqa # pylint: disable=super-init-not-called
        pass

    def resolve(self) -> str:
        return "main"
