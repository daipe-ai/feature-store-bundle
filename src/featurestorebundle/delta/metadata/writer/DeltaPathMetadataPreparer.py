from featurestorebundle.delta.PathCreator import PathCreator
from featurestorebundle.delta.PathSchemaMerger import PathSchemaMerger
from featurestorebundle.delta.metadata.schema import get_metadata_schema, get_metadata_pk_columns


class DeltaPathMetadataPreparer:
    def __init__(
        self,
        path_creator: PathCreator,
        path_schema_merger: PathSchemaMerger,
    ):
        self.__path_creator = path_creator
        self.__path_schema_merger = path_schema_merger

    def prepare(self, path: str):
        self.__path_creator.create_if_not_exists(path, get_metadata_schema(), get_metadata_pk_columns()[0].name)
        self.__path_schema_merger.merge(path, get_metadata_schema())
