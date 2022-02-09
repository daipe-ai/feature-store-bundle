from pyspark.sql import SparkSession


class DummySparkFactory:
    def create(self):
        class DummySparkSession(SparkSession):
            # pylint: disable=super-init-not-called
            def __init__(self, *args):
                pass

            @property
            def _sc(self):
                return DummySparkContext()

        class DummySparkContext:
            def getCheckpointDir(self):  # noqa pylint: disable=invalid-name
                return ""

            def setCheckpointDir(self, dirName):  # noqa pylint: disable=invalid-name
                pass

        return DummySparkSession()
