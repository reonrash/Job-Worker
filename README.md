# Job Ingestion Worker

A Kafka consumer that processes job data from a Kafka topic and ingests it into a PostgreSQL database with real-time matching and alerting.

## Overview

This worker consumes job messages from a Kafka topic, validates the data, and calls a PostgreSQL function that atomically handles job ingestion, duplicate detection, matching against user criteria, and alert generation.

## Features

- **Kafka Integration**: Consumes messages from a configurable Kafka topic
- **Database Connection Pooling**: Efficient PostgreSQL connection management
- **Atomic Operations**: All ingestion, matching, and alerting happens in a single transaction
- **Error Handling**: Robust error handling with retry mechanisms
- **Duplicate Detection**: Prevents duplicate job entries based on external_id
- **Real-time Processing**: Processes jobs as they arrive in the Kafka queue

## Prerequisites

- Python 3.7+
- PostgreSQL database (Supabase compatible)
- Kafka cluster
- Required PostgreSQL function `ingest_and_match` deployed in your database

## Installation

1. Clone or download the project files
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Configure environment variables (see Configuration section)
4. Run the worker:
   ```bash
   python ingestion_worker.py
   ```

## Configuration

Set the following environment variables or update the defaults in the code:

### Database Configuration
- `DB_HOST`: PostgreSQL host (e.g., "db.your-ref.supabase.co")
- `DB_NAME`: Database name (default: "postgres")
- `DB_USER`: Database username (default: "postgres")
- `DB_PASSWORD`: Database password
- `DB_PORT`: Database port (default: 5432)

### Kafka Configuration
- `KAFKA_BROKERS`: Comma-separated list of Kafka brokers (default: "localhost:9092")
- `KAFKA_TOPIC_RAW_JOBS`: Topic to consume from (default: "raw_jobs_queue")
- `KAFKA_GROUP_ID`: Consumer group ID (default: "job_ingestion_group_v1")

## Message Format

The worker expects JSON messages with the following structure:

```json
{
  "title": "Software Engineer",
  "company_name": "Example Corp",
  "external_id": "job_12345",
  "url": "https://example.com/jobs/12345",
  "location": "San Francisco, CA"
}
```

### Required Fields
- `title`: Job title
- `company_name`: Company name
- `external_id`: Unique identifier from the source system
- `url`: Job posting URL

### Optional Fields
- `location`: Job location (can be null/empty)

## Database Function

The worker calls a PostgreSQL function named `ingest_and_match` with the following signature:

```sql
ingest_and_match(
  company_name TEXT,
  external_id TEXT,
  title TEXT,
  location TEXT,
  url TEXT
) RETURNS BIGINT
```

This function should handle:
- Job insertion with duplicate detection
- Matching against user criteria
- Alert generation and logging
- Return the new job ID or 0 for duplicates

## Error Handling

- **JSON Decode Errors**: Messages with invalid JSON are skipped
- **Missing Required Fields**: Messages missing required data are skipped
- **Database Errors**: Transient errors trigger message retry via Kafka
- **Connection Failures**: Worker exits on startup connection failures

## Monitoring

The worker outputs logs for:
- Successful job processing
- Duplicate job detection
- Error conditions
- Database connection status
- Kafka consumer status

## Development

To extend the worker:

1. Modify the `process_message` function for additional validation
2. Update the `call_ingest_function` for different database schemas
3. Add additional error handling in the main consumer loop
4. Implement metrics collection for monitoring

## Troubleshooting

### Common Issues

1. **Database Connection Errors**: Verify credentials and network connectivity
2. **Kafka Connection Errors**: Check broker addresses and network access
3. **SSL Errors**: Ensure `sslmode='require'` is appropriate for your setup
4. **Function Not Found**: Verify the `ingest_and_match` function exists in your database

### Logs

Monitor the console output for detailed error messages and processing status.