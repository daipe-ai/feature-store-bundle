from logging import Logger

from daipecore.widgets.Widgets import Widgets
from py4j.protocol import Py4JJavaError


class CheckpointGuard:
    def __init__(self, logger: Logger, checkpoint_before_merge: bool, checkpoint_after_join: bool, widgets: Widgets):
        self.__logger = logger
        self.__checkpoint_before_merge = checkpoint_before_merge
        self.__checkpoint_after_join = checkpoint_after_join
        self.__widgets = widgets

    def should_checkpoint_before_merge(self):
        return self.__checkpoint_before_merge and not self.__is_dry_run()

    def should_checkpoint_after_join(self):
        return self.__checkpoint_after_join and not self.__is_dry_run()

    def __is_dry_run(self):
        try:
            return self.__widgets.get_value("dry_run") == "true"
        except Py4JJavaError:
            self.__logger.info("Dry run widget does not exist. Defaulting to `dry_run = False`")
            return False
