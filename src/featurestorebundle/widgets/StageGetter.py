from typing import Dict, List

from daipecore.function.input_decorator_function import input_decorator_function
from injecta.container.ContainerInterface import ContainerInterface
from py4j.protocol import Py4JError
from pyspark.dbutils import DBUtils

from featurestorebundle.widgets.WidgetsFactory import WidgetsFactory


@input_decorator_function
def get_stages():
    def wrapper(container: ContainerInterface) -> Dict[str, List[str]]:
        dbutils: DBUtils = container.get(DBUtils)
        try:
            notebooks_str = dbutils.widgets.get("notebooks")
        except Py4JError:
            notebooks_str = WidgetsFactory.all_notebooks_placeholder

        if notebooks_str == WidgetsFactory.all_notebooks_placeholder:
            return container.get_parameters().featurestorebundle.orchestration.stages

        notebooks_list = notebooks_str.split(",")
        if WidgetsFactory.all_notebooks_placeholder in notebooks_list:
            raise Exception(
                f"`{WidgetsFactory.all_notebooks_placeholder}` together with selected notebooks is not a valid option. Please select either `{WidgetsFactory.all_notebooks_placeholder}` only or a subset of notebooks"
            )

        stages = {}
        for stage_notebook in notebooks_list:
            stage, notebook = stage_notebook.split(":")
            if stage not in stages:
                stages[stage] = [notebook.lstrip()]
            else:
                stages[stage].append(notebook.lstrip())
        return stages

    return wrapper


@input_decorator_function
def get_stage(stage_key: str):
    def wrapper(container: ContainerInterface):
        stages = container.get_parameters().featurestorebundle.orchestration.stages
        return stages[stage_key]

    return wrapper
