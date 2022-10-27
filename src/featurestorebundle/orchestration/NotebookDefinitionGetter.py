from pathlib import Path
from box import Box


class NotebookDefinitionGetter:
    def __init__(self, orchestration: Box):
        self.__orchestration = orchestration

    def get(self, notebook_name: str) -> Box:
        for _, notebook_definitions in self.__orchestration.stages.items():
            for notebook_definition in notebook_definitions:
                if self.__get_notebook_name(notebook_definition.notebook) == notebook_name:
                    return notebook_definition

        raise Exception(f"Notebook {notebook_name} not found in orchestration definition")

    def __get_notebook_name(self, notebook_path: str):
        return Path(notebook_path).stem
