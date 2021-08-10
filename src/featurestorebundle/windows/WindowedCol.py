from pyspark.sql import Column, functions as f


class WindowedCol:
    def __init__(self, name: str, col: Column, agg_fun: callable, default_value=0):
        self.__name = name
        self._col = col
        self.__agg_fun = agg_fun
        self.__default_value = default_value

    def to_agg_windowed_column(self, is_window: Column, window: str) -> Column:
        col_name = self.agg_col_name(window)
        return self.__agg_fun(self.to_windowed_column(is_window)).alias(col_name)

    def agg_col_name(self, window: str) -> str:
        agg_name = self.__name.format(window=window)
        return agg_name

    def to_windowed_column(self, is_window: Column) -> Column:
        return self._fwhen(is_window, self._col).otherwise(self.__default_value)

    def _fwhen(self, *args, **kwargs) -> Column:
        return f.when(*args, **kwargs)
