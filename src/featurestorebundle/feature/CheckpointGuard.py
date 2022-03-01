from daipecore.widgets.Widgets import Widgets


class CheckpointGuard:
    def __init__(self, checkpoint_before_merge: bool, checkpoint_after_join: bool, widgets: Widgets):
        self.__checkpoint_before_merge = checkpoint_before_merge
        self.__checkpoint_after_join = checkpoint_after_join
        self.__widgets = widgets

    def should_checkpoint_before_merge(self):
        return self.__checkpoint_before_merge and self.__is_dry_run()

    def should_checkpoint_after_join(self):
        return self.__checkpoint_after_join and self.__is_dry_run()

    def __is_dry_run(self):
        return self.__widgets.get_value("dry_run") == "true"
