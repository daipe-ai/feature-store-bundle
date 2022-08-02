from dataclasses import dataclass


@dataclass(frozen=True)
class WidgetNames:
    all_notebooks_placeholder = "<all>"
    no_targets_placeholder = "<no target>"
    sample_value = "sample"
    entity_name = "entity_name"
    target_name = "target_name"
    timestamp_name = "timestamp"
    target_date_from_name = "target_date_from"
    target_date_to_name = "target_date_to"
    target_time_shift = "target_time_shift"
    notebooks_name = "notebooks"
    features_orchestration_id = "features_orchestration_id"
    sample_name = "sample_data"
