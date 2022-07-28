from pyspark.sql import SparkSession


class DummySparkFactory:
    def create(self):
        class DummySparkSession(SparkSession):
            def __init__(self, *args):  # noqa # pylint: disable=super-init-not-called
                pass

            @property
            def _sc(self):
                return DummySparkContext()

            @property
            def conf(self):
                return DummySparkConfig()

        class DummySparkContext:
            def getCheckpointDir(self):  # noqa pylint: disable=invalid-name
                return ""

            def setCheckpointDir(self, dirName):  # noqa pylint: disable=invalid-name
                pass

        class DummySparkConfig:
            def get(self, key, default=None):  # noqa pylint: disable=unused-argument
                return ""

        return DummySparkSession()
