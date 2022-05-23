# Feature Store bundle

**This package is distributed under the "DataSentics SW packages Terms of Use." See [license](https://raw.githubusercontent.com/daipe-ai/feature-store-bundle/master/LICENSE)**

Feature store bundle allows you to store features with metadata.

# Installation

```bash
poetry add feature-store-bundle
```

# Getting started

1. Define entities in `config.yaml` of your Daipe project

```yaml
parameters:
  featurestorebundle:
    entities:
      client:
        id_column: "client_id"
        id_column_type: "long"
      customer:
        id_column: "customer_id"
        id_column_type: "long"
```

2. Setup Feature store widgets in your Databricks feature notebook

```
# import daipe as dp

@dp.notebook_function()
def init_widgets(widgets_factory: dp.fs.WidgetsFactory):
    widgets_factory.create()
```

1. Use the widgets to select an entity and create a custom `feature` decorator for it

```python
import daipe as dp

entity = dp.fs.get_entity()
feature = dp.fs.feature_decorator_factory.create(entity)
```

2. Prepare your data

```python
# from pyspark.sql import SparkSession
# from datetime import datetime

@dp.transformation(display=True)
def load_data(spark: SparkSession):
    data = [
        {"client_id": 1, "date": datetime.strptime("2020-12-01", "%Y-%m-%d"), "amount": 121.44},
        {"client_id": 1, "date": datetime.strptime("2020-12-06", "%Y-%m-%d"), "amount": 21.44},
        {"client_id": 3, "date": datetime.strptime("2020-12-05", "%Y-%m-%d"), "amount": 321.44},
        {"client_id": 1, "date": datetime.strptime("2020-11-01", "%Y-%m-%d"), "amount": 121.44},
        {"client_id": 2, "date": datetime.strptime("2020-12-01", "%Y-%m-%d"), "amount": 421.44},
        {"client_id": 2, "date": datetime.strptime("2020-11-08", "%Y-%m-%d"), "amount": 121.44},
        {"client_id": 3, "date": datetime.strptime("2020-12-01", "%Y-%m-%d"), "amount": 221.44},
        {"client_id": 3, "date": datetime.strptime("2020-12-03", "%Y-%m-%d"), "amount": 221.44},
        {"client_id": 1, "date": datetime.strptime("2020-11-01", "%Y-%m-%d"), "amount": 21.44},
        {"client_id": 4, "date": datetime.strptime("2020-11-01", "%Y-%m-%d"), "amount": 21.54},
    ]
    return spark.createDataFrame(data).select("client_id", "date", "amount")
```

3. Add timestamps

```python
# import daipe as dp
# from pyspark.sql import DataFrame

@dp.transformation(
    dp.fs.with_timestamps_no_filter(
        load_data,
        entity,
    ),
    display=False
)
def data_with_timestamps(df: DataFrame):
    return df.cache()
```

4. Create features

```python
# import daipe as dp
# from pyspark.sql import DataFrame
# import pyspark.sql.functions as f

@dp.transformation(data_with_timestamps, display=True)
@feature(
    dp.fs.Feature("num_transactions", "Number of transactions", fillna_with=0),
    dp.fs.Feature("sum_amount", "Sum of amount loaned", fillna_with=0),
    category="financial",
)
def client_features(df: DataFrame):
    return (
        df
        .groupBy(entity.get_primary_key())
        .agg(
            f.count("amount").alias("num_transactions"),
            f.sum("amount").alias("sum_amount"),
        )
    )
```

5. Use the orchestration notebook in Databricks to write all the features in one go.
