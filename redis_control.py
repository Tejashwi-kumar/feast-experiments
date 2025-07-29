import asyncio
import threading
import logging
import os
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field
import redis.asyncio as redis
from confluent_kafka import Consumer, KafkaException, KafkaError

# --- Configuration ---
# It's best practice to load these from environment variables
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = int(os.environ.get("REDIS_PORT", 6379))
# This is the central key in Redis that all pods will monitor.
KAFKA_JOB_CONFIG_KEY = "kafka_ingestion_job:config"
# How often each pod checks Redis for state changes (in seconds)
STATE_CHECK_INTERVAL = 5

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(threadName)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# --- Application State ---
# This dictionary will hold the state for each individual pod
app_state = {
    "consumer_threads": [],
    "is_consuming": False,
    "current_config": None,
}

# --- Kafka Consumer Thread ---
# This runs in a separate thread to avoid blocking FastAPI's async event loop
class KafkaConsumerThread(threading.Thread):
    def __init__(self, config: dict):
        super().__init__()
        self.daemon = True  # Allows main thread to exit even if this thread is running
        self.consumer_config = {
            'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
            'group.id': config['group_id'],
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': True,
            'auto.commit.interval.ms': 5000
        }
        self.topic = config['topic']
        self._running = threading.Event()
        self._running.set()
        self.name = f"ConsumerThread-{self.topic}-{config['group_id']}"

    def run(self):
        """The main consumer loop."""
        consumer = Consumer(self.consumer_config)
        try:
            consumer.subscribe([self.topic])
            logger.info(f"Thread started and subscribed to topic '{self.topic}'.")
            while self._running.is_set():
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        logger.info(f"Reached end of partition for {msg.topic()} [{msg.partition()}]")
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Proper message
                    logger.info(f"Consumed message from {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    # --- YOUR MESSAGE PROCESSING LOGIC GOES HERE ---
                    # For example: process_ml_inference(msg.value())
        except Exception as e:
            logger.error(f"Exception in consumer thread: {e}", exc_info=True)
        finally:
            logger.info("Closing Kafka consumer.")
            consumer.close()

    def stop(self):
        """Signals the thread to stop."""
        logger.info("Stop signal received for consumer thread.")
        self._running.clear()

# --- Core Application Logic ---

async def start_consumption(config: dict):
    """Starts consumer threads on the current pod based on the provided config."""
    if app_state["is_consuming"]:
        logger.warning("Request to start consumption, but it's already running. Ignoring.")
        return

    logger.info(f"Starting consumption with config: {config}")
    num_threads = config.get("threads_per_pod", 1)
    
    threads = []
    for i in range(num_threads):
        thread = KafkaConsumerThread(config)
        threads.append(thread)
        thread.start()
        logger.info(f"Started consumer thread {i+1}/{num_threads}.")

    app_state["consumer_threads"] = threads
    app_state["is_consuming"] = True
    app_state["current_config"] = config
    logger.info("Consumption started successfully on this pod.")

async def stop_consumption():
    """Stops all running consumer threads on the current pod."""
    if not app_state["is_consuming"]:
        logger.warning("Request to stop consumption, but it's not running. Ignoring.")
        return

    logger.info("Stopping all consumer threads...")
    for thread in app_state["consumer_threads"]:
        thread.stop()

    # Wait for all threads to finish
    for thread in app_state["consumer_threads"]:
        thread.join(timeout=10) # Add a timeout to prevent hanging
        if thread.is_alive():
            logger.warning(f"Thread {thread.name} did not terminate gracefully.")

    app_state["consumer_threads"] = []
    app_state["is_consuming"] = False
    app_state["current_config"] = None
    logger.info("All consumer threads stopped and cleaned up on this pod.")

async def background_state_monitor(redis_conn: redis.Redis):
    """
    The heart of the worker. This long-running task monitors Redis for
    state changes and starts/stops consumption accordingly.
    """
    logger.info("Background state monitor started.")
    while True:
        try:
            job_config_raw = await redis_conn.get(KAFKA_JOB_CONFIG_KEY)
            job_config = json.loads(job_config_raw) if job_config_raw else {"status": "STOPPED"}
            
            job_status = job_config.get("status", "STOPPED")

            # --- State Transition Logic ---
            if job_status == "RUNNING" and not app_state["is_consuming"]:
                logger.info("State change detected: STOPPED -> RUNNING. Starting consumers.")
                await start_consumption(job_config)
            elif job_status == "STOPPED" and app_state["is_consuming"]:
                logger.info("State change detected: RUNNING -> STOPPED. Stopping consumers.")
                await stop_consumption()
            elif job_status == "RUNNING" and app_state["is_consuming"]:
                # Handle dynamic config changes (e.g., changing topic) without full restart
                if job_config != app_state["current_config"]:
                    logger.info("Configuration change detected. Restarting consumers with new config.")
                    await stop_consumption()
                    # Brief pause to allow graceful shutdown
                    await asyncio.sleep(2) 
                    await start_consumption(job_config)

        except Exception as e:
            logger.error(f"Error in background state monitor: {e}", exc_info=True)
        
        await asyncio.sleep(STATE_CHECK_INTERVAL)


# --- FastAPI Application ---

@asynccontextmanager
async def lifespan(app: FastAPI):
    # On startup
    logger.info("Application startup...")
    app.state.redis = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
    # Set initial state in Redis to STOPPED if not present
    await app.state.redis.setnx(KAFKA_JOB_CONFIG_KEY, json.dumps({"status": "STOPPED"}))
    # Start the background worker task
    asyncio.create_task(background_state_monitor(app.state.redis))
    yield
    # On shutdown
    logger.info("Application shutdown...")
    # Ensure consumers are stopped
    await stop_consumption()
    # Set state to STOPPED in Redis to prevent orphaned consumers on redeploy
    await app.state.redis.set(KAFKA_JOB_CONFIG_KEY, json.dumps({"status": "STOPPED"}))
    await app.state.redis.close()


app = FastAPI(
    lifespan=lifespan,
    title="Scalable Kafka Consumer Manager",
    description="An API to manage a distributed Kafka consumption job across multiple pods.",
)

class StartIngestionRequest(BaseModel):
    topic: str = Field(..., description="The Kafka topic to consume from.")
    group_id: str = Field(..., description="The consumer group ID.")
    threads_per_pod: int = Field(8, gt=0, le=16, description="Number of consumer threads to start on each pod.")

@app.post("/start", status_code=status.HTTP_202_ACCEPTED)
async def start_ingestion_job(request: StartIngestionRequest):
    """
    Starts the distributed ingestion job.
    This endpoint updates the central configuration in Redis, which all pods monitor.
    """
    logger.info(f"Received request to START job with config: {request.model_dump()}")
    config = {
        "status": "RUNNING",
        **request.model_dump()
    }
    await app.state.redis.set(KAFKA_JOB_CONFIG_KEY, json.dumps(config))
    return {"message": "Ingestion job start signal sent. All pods will begin consumption shortly."}

@app.post("/stop", status_code=status.HTTP_202_ACCEPTED)
async def stop_ingestion_job():
    """
    Stops the distributed ingestion job.
    This sets the central status to 'STOPPED', signaling all pods to shut down their consumers.
    """
    logger.info("Received request to STOP job.")
    config = {"status": "STOPPED"}
    await app.state.redis.set(KAFKA_JOB_CONFIG_KEY, json.dumps(config))
    return {"message": "Ingestion job stop signal sent. All pods will cease consumption."}

@app.get("/status", status_code=status.HTTP_200_OK)
async def get_ingestion_status():
    """
    Retrieves the current status of the ingestion job from the central Redis store.
    """
    config_raw = await app.state.redis.get(KAFKA_JOB_CONFIG_KEY)
    if not config_raw:
        return {"status": "UNKNOWN", "details": "No configuration found in Redis."}
    return json.loads(config_raw)

# To run this application:
# 1. Make sure you have a Kafka and Redis instance running.
# 2. Set environment variables if they are not on localhost (e.g., export KAFKA_BOOTSTRAP_SERVERS='your_kafka_host:9092')
# 3. Install dependencies: pip install fastapi uvicorn "redis[hiredis]" confluent-kafka
# 4. Run with uvicorn: uvicorn your_file_name:app --host 0.0.0.0 --port 8000
