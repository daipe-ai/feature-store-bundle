from functools import partial
from typing import List, Callable, Optional, Union

from pyspark.sql import Column, DataFrame

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.notebook.functions.time_windows import WindowedColumn, with_time_windows, is_past_time_window, resolve_column_type


class WindowedDataFrame(DataFrame):
    def __init__(self, df: DataFrame, entity: Entity, time_column_name_to_be_subtracted_from_timestamp: str, time_windows: List[str]):
        super().__init__(df._jdf, df.sql_ctx)  # noqa # pyre-ignore[6]
        self.__entity = entity
        self.__time_column_name = time_column_name_to_be_subtracted_from_timestamp
        self.__time_windows = time_windows

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if hasattr(attr, "__call__"):
            # Always return WindowedDataFrame instead of DataFrame
            def wrapper(*args, **kwargs):
                result = attr(*args, **kwargs)
                if isinstance(result, DataFrame):
                    return WindowedDataFrame(result, self.__entity, self.__time_column_name, self.__time_windows)

                return result

            return wrapper
        return attr

    def get_windowed_column_list(self, column_names: List[str]) -> List[str]:
        result = []
        for time_window in self.__time_windows:
            result.extend([col.format(time_window=time_window) for col in column_names])
        return result

    def apply_per_time_window(self, fun: Callable[["WindowedDataFrame", str], "WindowedDataFrame"]) -> DataFrame:
        wdf = self
        for time_window in self.__time_windows:
            wdf = fun(wdf, time_window)
        return wdf.df

    def is_time_window(self, time_window: str) -> Column:
        return is_past_time_window(
            resolve_column_type(self.df, self.__entity.time_column),
            resolve_column_type(self.df, self.__time_column_name),
            time_window,
        )

    def time_windowed(
        self,
        agg_columns_function: Callable[[str], List[Union[WindowedColumn, Column]]] = lambda x: [],
        non_agg_columns_function: Callable[[str], List[Column]] = lambda x: [],
        extra_group_keys: Optional[List[str]] = None,
        unnest_structs: bool = False,
    ) -> DataFrame:
        extra_group_keys = [] if extra_group_keys is None else extra_group_keys
        agg_cols, do_time_windows = self.__get_agg_cols(agg_columns_function)

        df = (
            with_time_windows(
                self.df,
                self.entity.time_column,
                self.__time_column_name,
                self.time_windows,
            ).cache()
            if do_time_windows
            else self.df
        )

        grouped_df = (df.groupby(self.entity.get_primary_key() + extra_group_keys).agg(*agg_cols)) if agg_cols else df
        non_agg_cols = self.__get_non_agg_cols(non_agg_columns_function)

        return self.__return_dataframe(grouped_df.select("*", *non_agg_cols), unnest_structs)

    @property
    def df(self) -> DataFrame:
        return DataFrame(self._jdf, self.sql_ctx)  # pyre-ignore[6]

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

    def __return_dataframe(self, df: DataFrame, unnest_structs: bool):
        structs = list(map(lambda col: col[0], filter(lambda col: col[1].startswith("struct"), df.dtypes)))
        non_structs = list(map(lambda col: col[0], (filter(lambda col: col[0] not in structs, df.dtypes))))

        return df.select(*non_structs, *map(lambda struct: f"{struct}.*", structs)) if unnest_structs else df

    def __get_agg_cols(self, agg_columns_function: Callable[[str], List[Union[WindowedColumn, Column]]]):
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
