from typing import Optional, List

from daipecore.decorator.InputDecorator import InputDecorator
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface

from featurestorebundle.entity.Entity import Entity
from featurestorebundle.notebook.WindowedDataFrame import WindowedDataFrame


@input_decorator_function
def make_windowed(input_decorator: InputDecorator, entity: Entity, time_column: str, custom_time_windows: Optional[List[str]] = None):
    def wrapper(container: ContainerInterface):
        default_time_windows = container.get_parameters().featurestorebundle.time_windows
        time_windows = default_time_windows if custom_time_windows is None else custom_time_windows

        return WindowedDataFrame(input_decorator.result, entity, time_column, time_windows)

    return wrapper
