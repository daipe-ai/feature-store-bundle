class CheckpointGuard:
    def __init__(self, checkpoint_result: bool, checkpoint_before_merge: bool, clean_checkpoint_dir: bool):
        self.__checkpoint_result = checkpoint_result
        self.__checkpoint_before_merge = checkpoint_before_merge
        self.__clean_checkpoint_dir = clean_checkpoint_dir

    def should_checkpoint_before_merge(self):
        return self.__checkpoint_before_merge

    def should_checkpoint_result(self):
        return self.__checkpoint_result

    def should_clean_checkpoint_dir(self):
        return self.__clean_checkpoint_dir
