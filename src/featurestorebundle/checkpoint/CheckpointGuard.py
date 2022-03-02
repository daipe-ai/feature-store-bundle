from featurestorebundle.widgets.DryRunHandler import DryRunHandler


class CheckpointGuard:
    def __init__(self, checkpoint_before_merge: bool, checkpoint_after_join: bool, dry_run_handler: DryRunHandler):
        self.__checkpoint_before_merge = checkpoint_before_merge
        self.__checkpoint_after_join = checkpoint_after_join
        self.__dry_run_handler = dry_run_handler

    def should_checkpoint_before_merge(self):
        return self.__checkpoint_before_merge and not self.__dry_run_handler.is_dry_run()

    def should_checkpoint_after_join(self):
        return self.__checkpoint_after_join and not self.__dry_run_handler.is_dry_run()
