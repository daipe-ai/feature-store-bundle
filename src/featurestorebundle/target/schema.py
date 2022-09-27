from featurestorebundle.entity.Entity import Entity


def get_id_column_name(entity: Entity):
    return entity.id_column


def get_time_column_name(entity: Entity):
    return entity.time_column


def get_target_id_column_name():
    return "target_id"
