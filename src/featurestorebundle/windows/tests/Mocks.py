from typing import Optional, Any

from featurestorebundle.windows.windowed_features import WindowedCol


class DataFrameMock:
    def __init__(self, data: dict):
        self.data = data

    def drop(self, *cols):
        for col in cols:
            del self.data[col]
        return self

    def withColumn(self, name, data):  # noqa: N802
        new_data = self.data.copy()
        new_data[name] = data
        return DataFrameMock(new_data)


class JavaCol:
    def __init__(self, cmd):
        if not cmd:
            cmd = "NULL"
        self.__cmd = cmd

    @property
    def cmd(self):
        return self.__cmd


class ColumnMock:
    def __init__(self, cmd, name=None):
        self._jc = JavaCol(cmd)
        self.name = name

    @property
    def cmd(self):
        return self._jc.cmd

    def isin(self, *lst):
        new_cmd = f"({self.cmd}) IN ({', '.join(lst)})"
        return ColumnMock(new_cmd)

    def cast(self, dtype: str):
        return ColumnMock(f"CAST ({self.cmd}) AS {dtype}")

    def alias(self, name: str):
        return ColumnMock(f"{self.cmd} AS `{name}`", name)

    def __repr__(self):
        return f"Column<'{self.cmd}'>"

    def __sub__(self, other: Any):
        if not isinstance(other, ColumnMock):
            other = ColumnMock(other)
        return ColumnMock(f"({self.cmd}) - {other.cmd})")

    def __ge__(self, other: Any):
        if not isinstance(other, ColumnMock):
            other = ColumnMock(other)
        return ColumnMock(f"({self.cmd}) >= {other.cmd})")


class ColumnWhenMock(ColumnMock):
    def otherwise(self, value: Optional[ColumnMock]):
        if not value:
            value = ColumnMock(None)
        return ColumnMock(f"{self.cmd} ELSE {value.cmd})")


def fwhen(condition: ColumnMock, value: ColumnMock):
    cmd = f"(CASE WHEN {condition.cmd} THEN {value.cmd}"
    return ColumnWhenMock(cmd)


def fcol(name: str) -> ColumnMock:
    return ColumnMock(name)


def flit(value: Any) -> ColumnMock:
    return ColumnMock(value)


def fsum(col: ColumnMock) -> ColumnMock:
    return fcol(f"sum({col.cmd})")


class WindowedColMock(WindowedCol):
    def _fwhen(self, *args, **kwargs) -> ColumnMock:
        return fwhen(*args, **kwargs)
