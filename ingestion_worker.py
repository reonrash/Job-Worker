import json
import os
import time
import threading
from dotenv import load_dotenv
from kafka import KafkaConsumer
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from job_normalization import normalize_job_data

# Load environment variables from .env file
load_dotenv()

# =================================================================
# CONFIGURATION
# =================================================================

# Database connection details (Replace these with your Supabase credentials)
DB_HOST = os.getenv("DB_HOST", "db.[YOUR_SUPABASE_REF].supabase.co")
DB_NAME = os.getenv("DB_NAME", "postgres")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "your_strong_password")
DB_PORT = os.getenv("DB_PORT", 5432)

# Kafka connection details
KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "localhost:9092").split(',')
KAFKA_TOPIC_RAW_JOBS = os.getenv("KAFKA_TOPIC_RAW_JOBS", "raw_jobs_queue")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "job_ingestion_group_v1")

# Connection pool settings
POOL_MIN_CONN = 2
POOL_MAX_CONN = 10

# SQL function name (The function deployed in Phase I)
INGEST_FUNCTION_NAME = "ingest_and_match"


# =================================================================
# DATABASE UTILITIES
# =================================================================

# Global connection pool instance
db_pool = None


def initialize_db_pool():
    """Initializes the PostgreSQL connection pool."""
    global db_pool
    print("Connecting to database...")
    try:
        db_pool = pool.SimpleConnectionPool(
            POOL_MIN_CONN,
            POOL_MAX_CONN,
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            port=DB_PORT,
            # Ensure connection uses SSL if required by Supabase
            sslmode='require'
        )
        # Test connection
        test_conn = db_pool.getconn()
        test_conn.cursor().execute("SELECT 1")
        db_pool.putconn(test_conn)
        print("✅ Database connected successfully")
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        # Exit if DB connection fails on startup
        raise


def call_ingest_function(job_data: dict) -> int:
    """
    Calls the atomic ingest_and_match function in PostgreSQL.

    Args:
        job_data: Dictionary containing validated job fields.

    Returns:
        The newly inserted job_id (BIGINT) or 0 if the job was a duplicate.
    """
    conn = None
    try:
        # Get a connection from the pool
        conn = db_pool.getconn()
        conn.set_session(autocommit=False)
        cur = conn.cursor()

        # Define the SQL call to the function deployed in Supabase
        # We use sql.Identifier and sql.Literal for safe parameter passing
        func_call = sql.SQL(
            "SELECT {}(%s, %s, %s, %s, %s)"
        ).format(sql.Identifier(INGEST_FUNCTION_NAME))

        # Execute the function call with the job data parameters
        cur.execute(
            func_call,
            (
                job_data['company_id'],
                job_data['external_id'],
                job_data['title'],
                job_data['location'],
                job_data['url']
            )
        )

        # Fetch the return value (the new job ID or 0)
        new_job_id = cur.fetchone()[0]

        # Commit the transaction (Ingestion, Matching, and Alert logging are all atomic)
        conn.commit()

        # The function handles all logic, so we just report the result
        if new_job_id > 0:
            print(
                f"SUCCESS: New Job ingested (ID: {new_job_id}) and alerts processed.")
        else:
            print(
                f"INFO: Job with external_id {job_data['external_id']} was a duplicate. Skipping.")

        return new_job_id

    except Exception as e:
        # Roll back if any error occurred during the atomic operation
        if conn:
            conn.rollback()
        print(f"CRITICAL ERROR during DB ingestion/matching: {e}")
        # Re-raise the exception to allow the Kafka handler to deal with the message (e.g., retry)
        raise
    finally:
        # Return the connection to the pool
        if conn:
            db_pool.putconn(conn)


# =================================================================
# KAFKA WORKER LOGIC
# =================================================================

def process_message(message_value: bytes):
    """Handles parsing and processing a single Kafka message."""
    try:
        # 1. Parse JSON data
        raw_data = json.loads(message_value.decode('utf-8'))
        print(
            f"Processing raw data for: {raw_data.get('title', 'Unknown Title')}")

        # 2. Basic Data Validation (adjust required fields based on your scraper output)
        required_fields = ['title', 'company', 'id', 'url']
        for field in required_fields:
            if not raw_data.get(field):
                raise ValueError(f"Missing required field: {field}")

        # Prepare data for the DB function (ensuring all required parameters are present)
        job_data = {
            'title': raw_data['title'],
            'company_id': raw_data['company'],
            'external_id': str(raw_data['id']),  # Ensure external_id is string
            'url': raw_data['url'],
            # Ensure location is present, even if empty/None
            'location': raw_data.get('location')
        }

        # Normalize the job data to include normalized_location
        job_data = normalize_job_data(job_data)

        # 3. Call the atomic ingestion and matching function
        new_job_id = call_ingest_function(job_data)

        # 4. (Optional but recommended): If a new job triggered alerts, send a message
        #    to the next queue (the Alert Notification Queue) here.
        #    This requires a second Kafka Producer connection, which we are omitting
        #    for simplicity now, but is necessary for a decoupled system.

        if new_job_id > 0:
            print(f"Successfully processed and matched Job ID: {new_job_id}")

    except json.JSONDecodeError as e:
        print(
            f"FATAL: Message corrupted (JSON decode error). Skipping message: {e}")
        # We choose to skip (commit offset) non-recoverable errors like bad JSON
    except ValueError as e:
        print(f"WARN: Message missing critical data. Skipping message: {e}")
    except Exception as e:
        # This catches DB errors or any other critical failure during processing.
        # It's up to you if you want to let the Kafka loop fail and retry (e.g., transient DB failure)
        print(
            f"CRITICAL PROCESSING ERROR: {e}. Message will be retried based on Kafka settings.")
        # Re-raise to potentially trigger Kafka retry/dead-letter logic
        raise


def heartbeat_logger():
    """Prints a heartbeat every 30 seconds to show the worker is alive."""
    start_time = time.time()
    while True:
        time.sleep(30)
        uptime = int(time.time() - start_time)
        print(f"💓 Worker alive - uptime: {uptime}s")


def start_worker():
    """Main function to initialize and run the Kafka consumer."""
    # 1. Initialize DB Pool
    initialize_db_pool()

    # Start heartbeat in background
    heartbeat_thread = threading.Thread(target=heartbeat_logger, daemon=True)
    heartbeat_thread.start()

    # 2. Initialize Kafka Consumer
    consumer = None
    try:
        print("Connecting to Kafka...")
        consumer = KafkaConsumer(
            KAFKA_TOPIC_RAW_JOBS,
            bootstrap_servers=KAFKA_BROKERS,
            group_id=KAFKA_GROUP_ID,
            # Start reading from the last committed offset
            auto_offset_reset='earliest',
            enable_auto_commit=False,  # We commit manually only on success
            value_deserializer=lambda x: x  # We handle JSON decoding in the processor
        )
        print(f"✅ Kafka connected to topic: {KAFKA_TOPIC_RAW_JOBS}")
        print("⏳ Waiting for messages...")

        # 3. Main Consumption Loop
        message_count = 0
        for message in consumer:
            try:
                message_count += 1
                print(f"📨 Processing message #{message_count}")
                # Process the message and acknowledge/commit the offset only on success
                process_message(message.value)
                consumer.commit()
                print(f"✅ Message #{message_count} processed successfully")
            except Exception as e:
                # If process_message raises an exception (e.g., DB error)
                print(f"❌ Message #{message_count} failed: {e}")
                # The message will be re-delivered based on Kafka configuration
                time.sleep(1)  # Simple backoff before continuing the loop

    except Exception as e:
        print(f"❌ FATAL Kafka or Worker Error: {e}")
    finally:
        if db_pool:
            db_pool.closeall()
            print("DB pool closed.")
        if consumer:
            consumer.close()
            print("Kafka consumer closed.")


if __name__ == "__main__":
    start_worker()
