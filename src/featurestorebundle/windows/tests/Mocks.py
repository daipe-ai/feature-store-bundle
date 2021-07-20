from typing import Optional

from featurestorebundle.windows.windowed_features import WindowedCol


class DataFrameMock:  # noqa: N801
    def __init__(self, data: dict):
        self.data = data

    @property
    def columns(self):
        return self.data.keys()

    def drop(self, *cols):
        for col in cols:
            del self.data[col]
        return self

    def select(self, *cols):
        new_data = self.data.copy()
        for col in cols:
            if col == "*":
                continue
            new_data[col.name] = col
        return DataFrameMock(new_data)

    def withColumn(self, name, data):  # noqa: N802
        new_data = self.data.copy()
        new_data[name] = data
        return DataFrameMock(new_data)

    def withColumnRenamed(self, old_name, new_name):  # noqa: N802
        new_data = self.data.copy()
        new_data[new_name] = new_data[old_name]
        del new_data[old_name]
        return DataFrameMock(new_data)


class JavaCol:
    def __init__(self, cmd):
        if not cmd:
            cmd = "NULL"
        self.__cmd = cmd

    @property
    def cmd(self):
        return self.__cmd

    def toString(self):  # noqa: N802
        return self.cmd


class Column_mock:  # noqa: N801
    def __init__(self, cmd, name=None):
        self._jc = JavaCol(cmd)
        self.name = name

    @property
    def cmd(self):
        return self._jc.cmd

    def isin(self, *lst):
        new_cmd = f"({self.cmd}) IN ({', '.join(lst)})"
        return Column_mock(new_cmd)

    def cast(self, dtype):
        return Column_mock(f"CAST ({self.cmd}) AS {dtype}")

    def alias(self, name):
        return Column_mock(f"{self.cmd} AS `{name}`", name)

    def __repr__(self):
        return f"Column<'{self.cmd}'>"

    def __sub__(self, other):
        if isinstance(other, str):
            other = Column_mock(other)
        return Column_mock(f"({self.cmd}) - {other.cmd})")

    def __ge__(self, other):
        if isinstance(other, str):
            other = Column_mock(other)
        return Column_mock(f"({self.cmd}) >= {other.cmd})")


class ColumnWhen_mock(Column_mock):  # noqa: N801
    def otherwise(self, value: Optional[Column_mock]):
        if not value:
            value = Column_mock(None)
        return Column_mock(f"{self.cmd} ELSE {value.cmd})")


def fwhen(condition: Column_mock, value: Column_mock):
    cmd = f"(CASE WHEN {condition.cmd} THEN {value.cmd}"
    return ColumnWhen_mock(cmd)


def fcol(name: str):
    return Column_mock(name)


def flit(value):
    return Column_mock(value)


def fsum(col: Column_mock):
    return fcol(f"sum({col.cmd})")


class WindowedColMock(WindowedCol):  # noqa: N801
    def to_agg_windowed_column(self, agg_fun: callable, is_window: Column_mock, window) -> Column_mock:
        col_name = self.agg_col_name(agg_fun, window)
        wcol = fwhen(is_window, self._col).otherwise(None)
        return fsum(wcol).alias(col_name)
