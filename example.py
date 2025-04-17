# This is an example feature definition file

from feast import Entity, Feature, FeatureView, FileSource, ValueType, Field
from feast.types import Float32, Int64

# Read data from parquet files. Parquet is convenient for local development mode. For
# production, you can use your favorite DWH, such as BigQuery. See Feast documentation
# for more info.
driver_hourly_stats = FileSource(
    path="data/driver_stats.parquet",
    event_timestamp_column="event_timestamp",
    created_timestamp_column="created",
    timestamp_field="event_timestamp"
)

# Define an entity for the driver. You can think of entity as a primary key used to
# fetch features.
driver = Entity(name="driver_id", value_type=ValueType.INT64, description="driver id", )

# Our parquet files contain sample data that includes a driver_id column, timestamps and
# three feature column. Here we define a Feature View that will allow us to serve this
# data to our model online.
driver_hourly_stats_view = FeatureView(
    name="testkafkastreamfv",
    entities=[driver],
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64)
    ],
    online=True,
    source=driver_hourly_stats,
    tags={}
)







if __name__=="__main__":
    from feast import FeatureStore
    # store = FeatureStore(repo_path="feast_init_dir/feature_repo")
    # store.apply([driver, driver_hourly_stats, driver_hourly_stats_view])
    # from datetime import datetime, timedelta
    # import pandas as pd
    #
    # from feast import FeatureStore
    #
    # # The entity dataframe is the dataframe we want to enrich with feature values
    # entity_df = pd.DataFrame.from_dict(
    #     {
    #         "driver_id": [1001, 1002, 1003],
    #         "label_driver_reported_satisfaction": [1, 5, 3],
    #         "event_timestamp": [
    #             datetime.now() - timedelta(minutes=11),
    #             datetime.now() - timedelta(minutes=36),
    #             datetime.now() - timedelta(minutes=73),
    #         ],
    #     }
    # )
    #
    # store = FeatureStore(repo_path="./feast_init_dir/feature_repo")
    #
    # training_df = store.get_historical_features(
    #     entity_df=entity_df,
    #     features=[
    #         "driver_hourly_stats:conv_rate",
    #         "driver_hourly_stats:acc_rate",
    #         "driver_hourly_stats:avg_daily_trips",
    #     ],
    # ).to_df()
    #
    # print("----- Feature schema -----\n")
    # print(training_df.info())
    #
    # print()
    # print("----- Example features -----\n")
    # print(training_df.head())
    from pprint import pprint
    from feast import FeatureStore

    store = FeatureStore(repo_path="./feast_init_dir/feature_repo")

    # store.delete_feature_view("testkafkastreamfv")

    feature_vector = store.get_online_features(
        features=[
            "driver_hourly_stats:conv_rate",
            "driver_hourly_stats:acc_rate",
        ],
        entity_rows=[{"driver_id":1007}],
    ).to_dict()

    pprint(feature_vector)

