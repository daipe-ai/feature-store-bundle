from typing import List

from pyspark.sql import Column


class WindowedCols:
    def __init__(self, cols: List = None):
        if not cols:
            cols = []
        self.__cols = cols

    def with_column(self, col_name: str, col: Column):
        return WindowedCols(self.__cols + [col.alias(col_name)])

    @property
    def cols(self):
        return self.__cols
