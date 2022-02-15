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
from featurestorebundle.entity.getter import get_entity
from featurestorebundle.feature.FeaturesStorage import FeaturesStorage
from featurestorebundle.notebook.decorator import feature_decorator_factory

entity = get_entity()
features_storage = FeaturesStorage()

feature_decorator = feature_decorator_factory.create(entity, features_storage)
```

2. Use the `feature decorator` to save features as you create them

```python
import daipe as dp

from pyspark.sql import functions as f
from pyspark.sql import DataFrame

@dp.transformation(dp.read_table("silver.tbl_loans"), display=True)
@feature_decorator(
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
        .withColumn("timestamp", f.lit(today))
    )
```

3. Write/Merge all features in one go

```python
import daipe as dp
from featurestorebundle.feature.writer.FeaturesWriter import FeaturesWriter

@dp.notebook_function()
def write_features(writer: FeaturesWriter):
    writer.write(features_storage)
```
