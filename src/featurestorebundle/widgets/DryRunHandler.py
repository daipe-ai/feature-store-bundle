from logging import Logger

from daipecore.widgets.Widgets import Widgets
from py4j.protocol import Py4JJavaError


class DryRunHandler:
    def __init__(self, logger: Logger, widgets: Widgets):
        self.__logger = logger
        self.__widgets = widgets

    def is_dry_run(self):
        try:
            return self.__widgets.get_value("dry_run") == "true"
        except Py4JJavaError:
            self.__logger.info("Dry run widget does not exist. Defaulting to `dry_run = False`")
            return False
