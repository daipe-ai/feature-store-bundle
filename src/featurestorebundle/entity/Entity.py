from pyspark.sql.types import DataType


class Entity:
    def __init__(self, name: str, id_column: str, id_column_type: DataType, time_column: str, time_column_type: DataType):

        self.name = name
        self.id_column = id_column
        self.id_column_type = id_column_type
        self.time_column = time_column
        self.time_column_type = time_column_type
