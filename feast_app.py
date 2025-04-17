from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from feast import FeatureStore, KafkaSource, StreamFeatureView, FileSource, Field
from feast.data_format import JsonFormat
from feast.types import Int64,Float32
from typing import List, Optional, Dict
import asyncio
import pandas as pd
from kafka import KafkaConsumer
import json
from datetime import datetime
from models import KafkaSourceRequest, StreamFeatureViewRequest, MessageBatch  # Import from models.py

app = FastAPI()
store = FeatureStore(repo_path="./feast_init_dir/feature_repo")  # Assuming your Feast repository is in the current directory
kafka_consumers = {}  # To store active Kafka consumers

driver_stats_batch_source = FileSource(
    name="driver_stats_source",
    path="data/driver_stats.parquet",
    timestamp_field="event_timestamp",
)

store.apply([driver_stats_batch_source])


@app.post("/create_kafka_source/")
async def create_kafka_source(request: KafkaSourceRequest):
    """Creates a KafkaSource in Feast."""
    # try:
    kafka_source = KafkaSource(
            name=request.name,
            kafka_bootstrap_servers=request.bootstrap_servers,
            topic=request.topic,
            timestamp_field=request.timestamp_field,
            batch_source=driver_stats_batch_source,
            message_format=JsonFormat(
        schema_json="driver_id integer, event_timestamp timestamp, conv_rate double, acc_rate double, created timestamp"
    ),
        )
    store.apply([kafka_source])
    return {"message": f"KafkaSource '{request.name}' created successfully."}
    # except Exception as e:
    #     raise HTTPException(status_code=500, detail=str(e))


@app.post("/create_stream_feature_view/")
async def create_stream_feature_view(request: StreamFeatureViewRequest):
    """Creates a StreamFeatureView in Feast."""
    try:
        stream_feature_view = StreamFeatureView(
            name=request.name,
            entities=[store.get_entity("driver_id")],
            schema=[Field(name="conv_rate", dtype=Float32),
                    Field(name="acc_rate", dtype=Float32)],
            source=store.get_data_source("testkafkasource"),
            online=True
        )
        store.apply([stream_feature_view])
        return {"message": f"StreamFeatureView '{request.name}' created successfully."}
    except Exception as e:
        pass
    raise HTTPException(status_code=500, detail=str(e))


async def consume_and_ingest(bootstrap_servers: str, topic: str, stream_feature_view_name: str):
    """
    Continuously consumes Kafka messages, converts them to a Pandas DataFrame,
    and ingests them into the specified StreamFeatureView.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='feast-ingestion-group',  # Unique group ID for ingestion
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    )
    print(
        f"Consumer started for topic: {topic} on servers: {bootstrap_servers} ingesting to {stream_feature_view_name}")
    try:
        while True:
            msg_pack = consumer.poll(timeout_ms=1000)
            records = []
            for tp, messages in msg_pack.items():
                for msg in messages:
                    records.append(msg.value)  # Append the message value (the data)
            if records:
                try:
                    df = pd.DataFrame(records)
                    print("Consumed DataFrame:")
                    print(df)
                    # Ingest the DataFrame into the StreamFeatureView
                    await ingest_df_to_stream_feature_view(df, stream_feature_view_name)  # Await the ingestion
                except Exception as ingest_error:
                    print(f"Error ingesting data: {ingest_error}")
            await asyncio.sleep(0.1)
    except asyncio.CancelledError:
        print(f"Consumer for topic {topic} stopped.")
    finally:
        consumer.close()


async def ingest_df_to_stream_feature_view(df: pd.DataFrame, stream_feature_view_name: str):
    """Ingests a Pandas DataFrame into the specified StreamFeatureView."""
    try:
        # Get the StreamFeatureView
        stream_feature_view = store.get_stream_feature_view(stream_feature_view_name)
        if stream_feature_view is None:
            raise ValueError(f"StreamFeatureView '{stream_feature_view_name}' not found.")

        # Ensure a timestamp column exists.  Use a default if 'event_timestamp' is not present.
        timestamp_field = 'event_timestamp' if 'event_timestamp' in df.columns else 'created_at'

        if timestamp_field not in df.columns:
            raise ValueError(
                f"Timestamp column '{timestamp_field}' not found in DataFrame.  Please ensure your Kafka messages include this field, or that you set timestamp_field correctly in KafkaSourceRequest")

        # Convert timestamp column to datetime if it's not already.  Crucial for Feast.
        if not pd.api.types.is_datetime64_any_dtype(df[timestamp_field]):
            df[timestamp_field] = pd.to_datetime(df[timestamp_field])

        # Perform the ingestion.  This is the key step.
        store.write_to_online_store(stream_feature_view_name, df)
        print(f"Successfully ingested {len(df)} rows into StreamFeatureView '{stream_feature_view_name}'.")
    except Exception as e:
        print(f"Error during ingestion to '{stream_feature_view_name}': {e}")
        raise  # Re-raise to log in consume_and_ingest


@app.post("/start_ingestion/")
async def start_ingestion(request: KafkaSourceRequest, stream_feature_view_name: str):
    """
    Starts a background task to continuously consume Kafka messages
    and ingest them into the specified StreamFeatureView.
    """
    ingestion_name = f"{request.name}-{stream_feature_view_name}"  # make a unique name
    if ingestion_name in kafka_consumers:
        raise HTTPException(
            status_code=400,
            detail=f"Ingestion process for '{ingestion_name}' is already running.",
        )

    task = asyncio.create_task(
        consume_and_ingest(request.bootstrap_servers, request.topic, stream_feature_view_name)
    )
    kafka_consumers[ingestion_name] = task
    return {
        "message": f"Started ingestion from Kafka topic '{request.topic}'"
                   f" to StreamFeatureView '{stream_feature_view_name}'."
    }


@app.post("/stop_ingestion/{ingestion_name}")
async def stop_ingestion(ingestion_name: str):
    """Stops a running ingestion process."""
    if ingestion_name in kafka_consumers:
        task = kafka_consumers.pop(ingestion_name)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        return {"message": f"Ingestion process '{ingestion_name}' stopped."}
    else:
        raise HTTPException(
            status_code=404, detail=f"Ingestion process '{ingestion_name}' not found."
        )


@app.post("/ingest_data/")  # Add this endpoint for manual ingestion
async def ingest_data(stream_feature_view_name: str, message_batch: MessageBatch):
    """
    Manually ingest a batch of data into a StreamFeatureView.  This is useful for testing
    or for ingesting data that is not coming from Kafka.  The primary use case
    for this endpoint is to provide a way to ingest data for testing purposes.
    """
    if not message_batch.messages:
        raise HTTPException(status_code=400, detail="No messages provided.")

    try:
        df = pd.DataFrame(message_batch.messages)
        await ingest_df_to_stream_feature_view(df, stream_feature_view_name)
        return {"message": f"Successfully ingested data into StreamFeatureView '{stream_feature_view_name}'."}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error ingesting data: {e}")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
