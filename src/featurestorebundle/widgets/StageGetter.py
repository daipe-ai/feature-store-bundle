from box import Box
from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from py4j.protocol import Py4JError
from pyspark.dbutils import DBUtils

from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory
from featurestorebundle.orchestration.NotebookDefinitionGetter import NotebookDefinitionGetter


@input_decorator_function
def get_stages():
    def wrapper(container: ContainerInterface) -> Box:
        dbutils: DBUtils = container.get(DBUtils)
        notebook_definition_getter: NotebookDefinitionGetter = container.get(NotebookDefinitionGetter)

        try:
            notebooks_str = dbutils.widgets.get("notebooks")
        except Py4JError:
            notebooks_str = WidgetsFactory.all_notebooks_placeholder

        if notebooks_str == WidgetsFactory.all_notebooks_placeholder:
            return container.get_parameters().featurestorebundle.orchestration.stages

        notebooks_list = notebooks_str.split(",")
        if WidgetsFactory.all_notebooks_placeholder in notebooks_list:
            raise Exception(
                f"`{WidgetsFactory.all_notebooks_placeholder}` together with selected notebooks is not a valid option. Please select "
                f"either `{WidgetsFactory.all_notebooks_placeholder}` only or a subset of notebooks"
            )

        stages = [stage.split(":")[0].strip() for stage in notebooks_list]
        notebooks = [notebook.split(":")[1].strip() for notebook in notebooks_list]
        stages_definition = {stage: [] for stage in stages}

        for stage, notebook in zip(stages, notebooks):
            notebook_definition = notebook_definition_getter.get(notebook)
            stages_definition[stage].append(notebook_definition)

        return Box(stages_definition)

    return wrapper


@input_decorator_function
def get_stage(stage_key: str):
    def wrapper(container: ContainerInterface):
        stages = container.get_parameters().featurestorebundle.orchestration.stages
        return stages[stage_key]

    return wrapper
