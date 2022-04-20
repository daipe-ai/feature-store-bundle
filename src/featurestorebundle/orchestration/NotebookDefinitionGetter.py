from box import Box


class NotebookDefinitionGetter:
    def __init__(self, orchestration: Box):
        self.__orchestration = orchestration

    def get(self, notebook_name: str) -> Box:
        for _, notebook_definitions in self.__orchestration.stages.items():
            for notebook_definition in notebook_definitions:
                # for orchestration backwards compatibility
                if isinstance(notebook_definition, str) and self.__get_notebook_name(notebook_definition) == notebook_name:
                    return Box({"notebook": notebook_definition, **self.__orchestration.notebook.defaults})

                if isinstance(notebook_definition, Box) and self.__get_notebook_name(notebook_definition.notebook) == notebook_name:
                    return self.__add_default_values_if_missing(notebook_definition)

        return Box({"notebook": notebook_name, **self.__orchestration.notebook.defaults})

    def __add_default_values_if_missing(self, notebook_definition: Box) -> Box:
        return Box({**self.__orchestration.notebook.defaults, **notebook_definition})

    def __get_notebook_name(self, notebook_path: str):
        return notebook_path.split("/")[-1]
