import json
from pyspark.dbutils import DBUtils


class DatabricksCurrentBranchResolver:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def resolve(self) -> str:
        try:
            context = json.loads(self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

            current_branch = context["extraContext"]["mlflowGitReference"]

        except BaseException as exception:  # noqa # pylint: disable=broad-except
            raise Exception("Cannot Resolve current branch") from exception

        return current_branch
