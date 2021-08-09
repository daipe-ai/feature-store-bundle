from pyspark.sql import Column, functions as f


class WindowedCol:
    def __init__(self, name: str, col: Column, agg_fun: callable, default_value=0):
        self.__name = name
        self._col = col
        self.__agg_fun = agg_fun
        self.__default_value = default_value

    def to_agg_windowed_column(self, is_window: Column, window) -> Column:
        col_name = self.agg_col_name(window)
        return self.__agg_fun(self.to_windowed_column(is_window)).alias(col_name)

    def agg_col_name(self, window) -> str:
        function_name = self.__agg_fun.__name__
        agg_name = self.__name.format(agg_fun=function_name, window=window)
        return agg_name

    def to_windowed_column(self, is_window: Column) -> Column:
        return f.when(is_window, self._col).otherwise(self.__default_value)
