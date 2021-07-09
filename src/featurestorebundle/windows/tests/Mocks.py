from typing import Optional

from featurestorebundle.windows.windowed_features import WindowedCol, WindowedColForAgg


class DataFrame_mock:  # noqa: N801
    def __init__(self, data: dict):
        self.data = data
        self.schema = data.keys()

    def drop(self, *cols):
        for col in cols:
            del self.data[col]
        return self

    def withColumn(self, name, data):  # noqa: N802
        self.data[name] = data
        return self

    def withColumnRenamed(self, old_name, new_name):  # noqa: N802
        self.data[new_name] = self.data[old_name]
        del self.data[old_name]
        return self


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
    def __init__(self, cmd):
        self._jc = JavaCol(cmd)

    @property
    def cmd(self):
        return self._jc.cmd

    def __str__(self):
        return f"Column<'{self.cmd}'>"

    def isin(self, *lst):
        new_cmd = f"({self.cmd}) IN ({', '.join(lst)})"
        return Column_mock(new_cmd)

    def cast(self, dtype):
        return Column_mock(f"CAST ({self.cmd}) AS {dtype}")

    def alias(self, name):
        return f"{self.cmd} AS `{name}`"


class ColumnWhen_mock(Column_mock):  # noqa: N801
    def otherwise(self, value: Optional[Column_mock]):
        if not value:
            value = Column_mock(None)
        return Column_mock(f"{self.cmd} ELSE {value.cmd}")


def fwhen(condition: Column_mock, value: Column_mock):
    cmd = f"CASE WHEN {condition.cmd} THEN {value.cmd}"
    return ColumnWhen_mock(cmd)


def fcol(name):
    return Column_mock(name)


class WindowedCol_mock(WindowedCol):  # noqa: N801
    def _expr(self, *args, **kwargs):
        return fcol(args[0])


class WindowedColForAgg_mock(WindowedColForAgg):  # noqa: N801
    def _expr(self, *args, **kwargs):
        return fcol(args[0])


def fsum(x):
    return fcol(f"sum({str(x)[8:-2]})")
