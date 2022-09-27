import json
from pyspark.dbutils import DBUtils


class DatabricksRepositoryUrlResolver:
    def __init__(self, dbutils: DBUtils):
        self.__dbutils = dbutils

    def resolve(self) -> str:
        try:
            context = json.loads(self.__dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())

            repository_url = context["extraContext"]["mlflowGitUrl"]

        except BaseException as exception:  # noqa # pylint: disable=broad-except
            raise Exception("Cannot resolve repository URL") from exception

        return repository_url
