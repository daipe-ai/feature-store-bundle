class CheckpointGuard:
    def __init__(self, checkpoint_result: bool, checkpoint_before_merge: bool, checkpoint_after_join: bool):
        self.__checkpoint_result = checkpoint_result
        self.__checkpoint_before_merge = checkpoint_before_merge
        self.__checkpoint_after_join = checkpoint_after_join

    def should_checkpoint_before_merge(self):
        return self.__checkpoint_before_merge

    def should_checkpoint_after_join(self):
        return self.__checkpoint_after_join

    def should_checkpoint_result(self):
        return self.__checkpoint_result
