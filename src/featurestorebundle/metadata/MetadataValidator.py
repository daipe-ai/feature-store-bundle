from featurestorebundle.feature.FeatureList import FeatureList
from featurestorebundle.feature.FeatureListFactory import FeatureListFactory
from featurestorebundle.metadata.reader.MetadataReader import MetadataReader


class MetadataValidator:
    _immutable_metadata_fields = ["start_date", "frequency", "dtype", "fillna_value", "fillna_value_type"]

    def __init__(self, metadata_reader: MetadataReader, feature_list_factory: FeatureListFactory):
        self.__metadata_reader = metadata_reader
        self.__feature_list_factory = feature_list_factory

    def validate(self, feature_list: FeatureList):
        incoming_feature_list = feature_list
        current_feature_list = self.__get_current_feature_list()

        self.__check_no_duplicates_present(current_feature_list, incoming_feature_list)
        self.__check_immutable_fields_did_not_change(current_feature_list, incoming_feature_list)

    def __check_no_duplicates_present(self, current_feature_list: FeatureList, incoming_feature_list: FeatureList):
        for incoming_feature in incoming_feature_list.get_all():
            for current_feature in current_feature_list.get_all():
                if (
                    incoming_feature.name == current_feature.name
                    and incoming_feature.template.location != current_feature.template.location
                ):
                    raise Exception(
                        f"Duplicate feature detected {current_feature.name} is already registered "
                        f"for location {current_feature.template.location}"
                    )

    def __check_immutable_fields_did_not_change(self, current_feature_list: FeatureList, incoming_feature_list: FeatureList):
        for incoming_feature in incoming_feature_list.get_all():
            if not current_feature_list.contains_feature(incoming_feature.name):
                continue

            current_feature = current_feature_list.get_by_name(incoming_feature.name)

            for field in self._immutable_metadata_fields:
                incoming_field_value = getattr(incoming_feature, field, None) or getattr(incoming_feature.template, field, None)
                current_field_value = getattr(current_feature, field, None) or getattr(current_feature.template, field, None)

                if current_field_value is not None and current_field_value != incoming_field_value:
                    raise Exception(
                        f"Metadata field {field} for feature {incoming_feature.name} cannot be changed "
                        f"from {current_field_value} to {incoming_field_value}"
                    )

    def __get_current_feature_list(self) -> FeatureList:
        metadata = self.__metadata_reader.read()

        return self.__feature_list_factory.create(metadata)
