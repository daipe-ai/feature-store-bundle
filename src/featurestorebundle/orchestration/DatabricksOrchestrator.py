import uuid
from typing import List, Optional
from logging import Logger
from box import Box
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import DataFrame
from pyspark.dbutils import DBUtils
from featurestorebundle.delta.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.orchestration.NotebookTask import NotebookTask
from featurestorebundle.orchestration.NotebookTasksFactory import NotebookTasksFactory
from featurestorebundle.orchestration.Serializator import Serializator
from featurestorebundle.orchestration.PostActionsRunner import PostActionsRunner
from featurestorebundle.feature.writer.FeaturesWriter import FeaturesWriter
from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory
from featurestorebundle.checkpoint.CheckpointGuard import CheckpointGuard
from featurestorebundle.checkpoint.CheckpointDirHandler import CheckpointDirHandler


# pylint: disable=too-many-instance-attributes
class DatabricksOrchestrator:
    def __init__(
        self,
        logger: Logger,
        orchestration_stages: Box,
        num_parallel: int,
        dbutils: DBUtils,
        notebook_tasks_factory: NotebookTasksFactory,
        serializator: Serializator,
        features_writer: FeaturesWriter,
        features_preparer: FeaturesPreparer,
        post_actions_runner: PostActionsRunner,
        checkpoint_guard: CheckpointGuard,
        checkpoint_dir_handler: CheckpointDirHandler,
    ):
        self.__logger = logger
        self.__orchestration_stages = orchestration_stages
        self.__num_parallel = num_parallel
        self.__dbutils = dbutils
        self.__notebook_tasks_factory = notebook_tasks_factory
        self.__serializator = serializator
        self.__features_writer = features_writer
        self.__features_preparer = features_preparer
        self.__post_actions_runner = post_actions_runner
        self.__checkpoint_guard = checkpoint_guard
        self.__checkpoint_dir_handler = checkpoint_dir_handler

    def orchestrate(self, stages: Optional[Box] = None):
        stages = self.__orchestration_stages if stages is None else stages

        self.__logger.info("Starting features orchestration")

        for stage, notebook_definitions in stages.items():
            self.__logger.info(f"Running stage {stage}")

            notebook_tasks = self.__notebook_tasks_factory.create(notebook_definitions)

            features_storage = self.__orchestrate_stage(notebook_tasks)

            self.__features_writer.write(features_storage)

            self.__logger.info(f"Stage {stage} done")

        self.__logger.info("Features orchestration done")

        self.__post_actions_runner.run()

        if self.__checkpoint_guard.should_clean_checkpoint_dir():
            self.__checkpoint_dir_handler.clean_checkpoint_dir()

    def _prepare_dataframe(self, notebook_definitions: List[Box]) -> DataFrame:
        self.__logger.info("Running notebooks")

        notebook_tasks = self.__notebook_tasks_factory.create(notebook_definitions)
        features_storage = self.__orchestrate_stage(notebook_tasks)

        return self.__features_preparer.prepare(features_storage)

    def __orchestrate_stage(self, notebook_tasks: List[NotebookTask]) -> FeaturesStorage:
        orchestration_id = uuid.uuid4().hex

        futures = self.__submit_notebooks_parallel(notebook_tasks, orchestration_id)

        for future in futures:
            if future.exception() is not None:
                raise future.exception()

        features_storage = self.__serializator.deserialize(orchestration_id)

        return features_storage

    def __submit_notebooks_parallel(self, notebooks: List[NotebookTask], orchestration_id: str):
        with ThreadPoolExecutor(max_workers=self.__num_parallel) as executor:
            return [executor.submit(self.__submit_notebook, notebook, orchestration_id) for notebook in notebooks]

    def __submit_notebook(self, notebook: NotebookTask, orchestration_id: str):
        try:
            return self.__dbutils.notebook.run(
                notebook.path, notebook.timeout, {WidgetsFactory.features_orchestration_id: orchestration_id, **notebook.parameters}
            )

        except Exception:  # noqa pylint: disable=broad-except
            if notebook.retry < 1:
                self.__logger.error(f"Notebook {notebook.path} failed")
                raise

        notebook.retry = notebook.retry - 1

        return self.__submit_notebook(notebook, orchestration_id)
