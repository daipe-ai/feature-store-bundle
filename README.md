# Feature Store bundle

**This package is distributed under the "DataSentics SW packages Terms of Use." See [license](https://raw.githubusercontent.com/daipe-ai/feature-store-bundle/master/LICENSE)**

Feature store bundle allows you to store features with metadata.

# Installation

```bash
poetry add feature-store-bundle
```

# Getting started


1. Define entity and custom `feature decorator`

```python
from pyspark.sql import types as t
from daipecore.decorator.DecoratedDecorator import DecoratedDecorator
from featurestorebundle.entity.Entity import Entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator.feature import feature

entity = Entity(
    name="client",
    id_column="UserName",
    id_column_type=t.StringType(),
    time_column="run_date",
    time_column_type=t.DateType(),
)

@DecoratedDecorator
class client_feature(feature):  # noqa N081
    def __init__(self, *args, category=None):
        super().__init__(*args, entity=entity, category=category, features_storage=features_storage)
```

2. Use the `feature decorator` to save features as you create them

```python
from pyspark.sql import functions as f
from pyspark.sql import DataFrame
from datalakebundle.imports import transformation, read_table

@transformation(read_table("silver.tbl_loans"), display=True)
@client_feature(
    ("Age", "Client's age"),
    ("Gender", "Client's gender"),
    ("WorkExperience", "Client's work experience"),
    category="personal",
)
def client_personal_features(df: DataFrame):
    return (
        df.select("UserName", "Age", "Gender", "WorkExperience")
        .groupBy("UserName")
        .agg(
            f.max("Age").alias("Age"),
            f.first("Gender").alias("Gender"),
            f.first("WorkExperience").alias("WorkExperience"),
        )
        .withColumn("run_date", f.lit(today))
    )
```

3. Write/Merge all features in one go

```python
from datalakebundle.imports import notebook_function
from featurestorebundle.delta.DeltaWriter import DeltaWriter

notebook_function()
def write_features(writer: DeltaWriter):
    writer.write_latest(features_storage)
```
