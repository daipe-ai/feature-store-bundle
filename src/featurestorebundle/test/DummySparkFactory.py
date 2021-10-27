from pyspark.sql import SparkSession


class DummySparkFactory:
    def create(self):
        class DummySparkSession(SparkSession):
            # pylint: disable=super-init-not-called
            def __init__(self, *args):
                pass

        return DummySparkSession()
