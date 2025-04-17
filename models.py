from pydantic import BaseModel
from typing import Dict, List, Optional

class KafkaSourceRequest(BaseModel):
    name: str
    bootstrap_servers: str
    topic: str
    timestamp_field: str
    message_format: str = "json"  # Or "avro", etc.
    schema: Optional[Dict] = None  # Required for formats like avro

class StreamFeatureViewRequest(BaseModel):
    name: str
    entities: List[str]
    features: List[str]
    source: str  # Name of the KafkaSource created earlier
    ttl: Optional[str] = None

class MessageBatch(BaseModel):
    messages: List[Dict]