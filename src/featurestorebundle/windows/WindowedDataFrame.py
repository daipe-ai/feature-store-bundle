from functools import partial
from typing import List, Callable, Optional

from pyspark.sql import functions as f, Column
from pyspark.sql import DataFrame

from featurestorebundle.windows.functions import WindowedColumn
from featurestorebundle.windows.windowed_features import with_time_windows


class WindowedDataFrame(DataFrame):
    def __init__(self, df: DataFrame, entity, time_column: str, time_windows: List[str]):
        super().__init__(df._jdf, df.sql_ctx)
        self.__entity = entity
        self.__time_column = time_column
        self.__time_windows = time_windows

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if hasattr(attr, "__call__"):

            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    return WindowedDataFrame(result, self.__entity, self.__time_column, self.__time_windows)

                return result

            return wrapper
        return attr

    def get_windowed_column_list(self, column_names: List[str]) -> List[str]:
        result = []
        for time_window in self.__time_windows:
            result.extend([col.format(time_window=time_window) for col in column_names])
        return result

    def apply_per_time_window(self, fun: Callable[[DataFrame, str], DataFrame]) -> DataFrame:
        wdf = self
        for time_window in self.__time_windows:
            wdf = fun(wdf, time_window)
        return wdf.df

    def time_windowed(
        self,
        agg_columns_function: Callable[[str], List[WindowedColumn]] = lambda x: [],
        non_agg_columns_function: Callable[[str], List[Column]] = lambda x: [],
        extra_group_keys: Optional[List[str]] = None,
    ) -> DataFrame:
        extra_group_keys = [] if extra_group_keys is None else extra_group_keys
        df = self.df

        agg_cols, do_time_windows = self.__get_agg_cols(agg_columns_function)

        df = (
            with_time_windows(
                df,
                self.__time_column,
                f.col(self.entity.time_column).cast("timestamp"),
                self.time_windows,
            ).cache()
            if do_time_windows
            else df
        )

        grouped_df = (df.groupby(self.entity.get_primary_key() + extra_group_keys).agg(*agg_cols)) if agg_cols else df

        non_agg_cols = self.__get_non_agg_cols(non_agg_columns_function)

        return grouped_df.select("*", *non_agg_cols)

    @property
    def df(self) -> DataFrame:
        return DataFrame(self._jdf, self.sql_ctx)

    @property
    def entity(self):
        return self.__entity

    @property
    def primary_key(self):
        return self.__entity.get_primary_key()

    @property
    def time_column(self):
        return self.__time_column

    @property
    def time_windows(self):
        return self.__time_windows

    def __get_agg_cols(self, agg_columns_function: Callable[[str], List[WindowedColumn]]):
        do_time_windows = False

        def resolve_partial(window: str, partial_col: Callable[[str], WindowedColumn]):
            return partial_col(window)

        agg_cols = []
        for time_window in self.time_windows:
            agg_output = agg_columns_function(time_window)

            cols = filter(lambda x: isinstance(x, Column), agg_output)
            partial_cols = filter(lambda x: isinstance(x, partial), agg_output)

            agg_cols.extend(cols)
            agg_cols_len = len(agg_cols)

            resolver = partial(resolve_partial, time_window)
            agg_cols.extend(map(resolver, partial_cols))

            if len(agg_cols) > agg_cols_len:
                do_time_windows = True

        return agg_cols, do_time_windows

    def __get_non_agg_cols(self, non_agg_columns_function: Callable[[str], List[Column]]) -> List[Column]:
        non_agg_cols = []
        for time_window in self.time_windows:
            non_agg_cols.extend(non_agg_columns_function(time_window))
        return non_agg_cols
