import uuid
from typing import List
from logging import Logger
from box import Box
from pyspark.dbutils import DBUtils
from concurrent.futures import ThreadPoolExecutor

from featurestorebundle.delta.EmptyDataFrameCreator import EmptyDataFrameCreator
from featurestorebundle.delta.feature.schema import get_feature_store_initial_schema
from featurestorebundle.entity.getter import get_entity
from featurestorebundle.feature.FeaturesPreparer import FeaturesPreparer
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.orchestration.NotebookTask import NotebookTask
from featurestorebundle.orchestration.NotebookTasksFactory import NotebookTasksFactory
from featurestorebundle.orchestration.Serializator import Serializator
from featurestorebundle.feature.writer.FeaturesWriter import FeaturesWriter

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
        empty_dataframe_creator: EmptyDataFrameCreator,
    ):
        self.__logger = logger
        self.__orchestration_stages = orchestration_stages
        self.__num_parallel = num_parallel
        self.__dbutils = dbutils
        self.__notebook_tasks_factory = notebook_tasks_factory
        self.__serializator = serializator
        self.__features_writer = features_writer
        self.__features_preparer = features_preparer
        self.__empty_dataframe_creator = empty_dataframe_creator

    def orchestrate(self):
        self.__logger.info("Starting features orchestration")

        for stage, notebooks in self.__orchestration_stages.items():
            self.__logger.info(f"Running stage {stage}")

            notebook_tasks = self.__notebook_tasks_factory.create(notebooks)

            features_storage = self.__orchestrate_stage(notebook_tasks)
            self.__features_writer.write(features_storage)

            self.__logger.info(f"Stage {stage} done")

        self.__logger.info("Features orchestration done")

    def prepare_dataframe(self, notebooks: List[str]):
        notebook_tasks = self.__notebook_tasks_factory.create(notebooks)
        features_storage = self.__orchestrate_stage(notebook_tasks)
        entity = get_entity()
        self.__features_preparer.prepare(
            entity,
            self.__empty_dataframe_creator.create(get_feature_store_initial_schema(entity)),
            features_storage,
            entity.get_primary_key(),
        )

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
                notebook.path, notebook.timeout, {"daipe_features_orchestration_id": orchestration_id, **notebook.parameters}
            )

        except Exception:  # noqa pylint: disable=broad-except
            if notebook.retry < 1:
                self.__logger.error(f"Notebook {notebook.path} failed")
                raise

        notebook.retry = notebook.retry - 1

        return self.__submit_notebook(notebook, orchestration_id)
