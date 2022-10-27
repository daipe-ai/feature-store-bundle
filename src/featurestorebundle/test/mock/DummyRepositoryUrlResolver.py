from featurestorebundle.databricks.repos.DatabricksRepositoryUrlResolver import DatabricksRepositoryUrlResolver


class DummyRepositoryUrlResolver(DatabricksRepositoryUrlResolver):
    def resolve(self) -> str:
        return "https://test_repo.git"
