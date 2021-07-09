from pyspark.sql import Column
from featurestorebundle.windows.WindowedCol import WindowedCol


class WindowedColForAgg(WindowedCol):
    def __init__(self, name: str, col: Column, agg_fun: callable):
        super().__init__(name, col)
        self.__agg_fun = agg_fun

    def as_col(self, window) -> Column:
        col_name = self.col_name(window)
        return self.__agg_fun(super().as_col(window)).alias(col_name)

    def with_changed_col(self, new_col: Column):
        return WindowedColForAgg(self._name, new_col, self.__agg_fun)
