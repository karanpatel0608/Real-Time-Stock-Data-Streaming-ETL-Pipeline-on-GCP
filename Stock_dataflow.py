  
from __future__ import annotations
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import gcsio
import json
import time
import logging
from datetime import datetime
import random
from apache_beam.io.gcp.internal.clients import bigquery

# # --- Data Cleaning and Transformation Logic (No Changes Here) ---
def parse_and_clean(element_str: str) -> beam.pvalue.TaggedOutput | dict:
    """
    Parses the raw JSON message, cleans/transforms, and handles errors.
    """
    try:
        record = json.loads(element_str)

        if not record.get("ticker") or record.get("price") is None:
            raise ValueError("Missing critical fields: ticker or price is null")

        ticker = record["ticker"].strip().upper()
        trade_type = str(record.get("trade_type", "UNKNOWN")).upper()
        order_type = str(record.get("order_type", "UNKNOWN")).upper()
        client_category = str(record.get("client_category", "UNKNOWN")).upper()
        broker_id = str(record.get("broker_id")).strip()
        trade_status = str(record.get("trade_status", "UNKNOWN")).upper()
        
        try:
            price_str = str(record["price"]).strip()
            price = float(''.join(filter(lambda x: x.isdigit() or x == '.', price_str)))
        except (ValueError, TypeError):
            price = 0.0

        trade_volume = int(record.get("trade_volume", 0) or 0)
        if trade_volume < 0: trade_volume = 0

        latency = int(record.get("execution_latency_ms", 0) or 0)
        if latency < 0: latency = 0
            
        trade_value = round(price * trade_volume, 2)

        ts = record.get("timestamp")
        try:
            if isinstance(ts, (int, float)):
                if ts > 10**12:  # Milliseconds
                    dt_object = datetime.utcfromtimestamp(ts / 1000)
                else:  # Seconds
                    dt_object = datetime.utcfromtimestamp(ts)
            else:
                dt_object = datetime.fromisoformat(str(ts).replace('Z', ''))
            trade_timestamp = dt_object.isoformat()
        except (ValueError, TypeError):
            trade_timestamp = datetime.utcnow().isoformat()
        
        clean_record = {
            "trade_id": record.get("trade_id"), "ticker_symbol": ticker, "price": price,
            "trade_volume": trade_volume, "trade_value": trade_value, "trade_type": trade_type,
            "order_type": order_type, "client_id": record.get("client_id"), 
            "client_category": client_category, "client_location": record.get("client_location", "UNKNOWN"),
            "broker_id": broker_id, "execution_latency_ms": latency, "trade_status": trade_status,
            "market_session": record.get("market_session", "UNKNOWN"), "sector": record.get("sector", "UNKNOWN"),
            "trade_timestamp": trade_timestamp, "processing_timestamp": datetime.utcnow().isoformat()
        }
        
        yield clean_record

    except Exception as e:
        logging.error(f"Error processing record: {element_str}. Error: {e}")
        yield beam.pvalue.TaggedOutput('dead_letter', element_str)

# # --- The Beam Pipeline (Refactored Structure) ---
# def run():
#     pipeline_options = PipelineOptions(streaming=True)
#     gc_options = pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    
#     # Define resource names
#     # project_id = gc_options.project
#     input_topic = "projects/orbital-bee-462505-v9/topics/stock_data"
#     raw_archive_path = "gs://stock-project-data/stock-datalake"
#     dead_letter_path = "gs://stock-project-data/stock-deadletter"
#     bq_table_spec = "orbital-bee-462505-v9.stock_data.stock_trades_clean"

#     with beam.Pipeline(options=pipeline_options) as p:
#         # Step 1: Ingest from Pub/Sub
#         raw_messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
#             topic=input_topic
#         ).with_output_types(bytes)

#         # --- Branch 1: Raw Data Archival to GCS (with Windowing and Triggers) ---
#         # This branch works on the original 'raw_messages' PCollection of bytes.
#         (raw_messages
#             | "WindowRawInto1Minute" >> beam.WindowInto(
#                 beam.window.FixedWindows(60),  # 1 minute window
#                 trigger=beam.trigger.Repeatedly(
#                     beam.trigger.AfterAny(
#                         beam.trigger.AfterWatermark(early=beam.trigger.AfterProcessingTime(30)),
#                         beam.trigger.AfterCount(100)
#                     )
#                 ),
#                 accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
#                 allowed_lateness=60
#             )
#             | "DecodeForArchive" >> beam.Map(lambda x: x.decode("utf-8"))
#             | "WriteRawToGCS" >> beam.io.WriteToText(
#                 raw_archive_path,
#                 file_name_suffix=".jsonl",
#                 shard_name_template="-SS-of-NN"
#             )
#         )

#         # --- Branch 2: Clean, Transform, and Stream to BigQuery ---
#         # This branch starts with decoding the raw messages into strings.
#         decoded_messages = raw_messages | "DecodeMessages" >> beam.Map(lambda x: x.decode("utf-8"))
        
#         # Apply the cleaning function and use TaggedOutput to create two PCollections: 'clean' and 'dead_letter'
#         processed_results = decoded_messages | "ParseAndClean" >> beam.ParDo(parse_and_clean).with_outputs('dead_letter', main='clean')
        
#         clean_records = processed_results.clean
#         dead_letter_records = processed_results.dead_letter
        
#         # Sink 1 for Branch 2: Write clean records to BigQuery (no windowing needed)
#         (clean_records | "WriteCleanToBigQuery" >> beam.io.WriteToBigQuery(
#             table=bq_table_spec,
#             write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
#             create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
#             )
#         )
        
#         # Sink 2 for Branch 2: Write dead-letter records to GCS (with windowing and triggers)
#         (dead_letter_records
#             | "WindowDeadLetterInto1Minute" >> beam.WindowInto(
#                 beam.window.FixedWindows(60),  # 1 minute window
#                 trigger=beam.trigger.Repeatedly(
#                     beam.trigger.AfterAny(
#                         beam.trigger.AfterWatermark(early=beam.trigger.AfterProcessingTime(30)),
#                         beam.trigger.AfterCount(50)
#                     )
#                 ),
#                 accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
#                 allowed_lateness=60
#             )
#             | "WriteDeadLetterToGCS" >> beam.io.WriteToText(
#                 dead_letter_path,
#                 file_name_suffix=".txt"
#             )
#         )

# if __name__ == "__main__":
#     logging.getLogger().setLevel(logging.INFO)
#     run()




# --- The custom DoFn to write a batch of messages to a GCS file ---
# This replaces the need for beam.io.WriteToText for our streaming sinks
class WriteBatchToFile(beam.DoFn):
    def __init__(self, output_path, file_suffix):
        self.output_path = output_path
        self.file_suffix = file_suffix

    def process(self, batch, window=beam.DoFn.WindowParam):
        """
        Receives a batch of messages for a given window and writes them to a GCS file.
        'batch' is a tuple: (key, iterable_of_elements)
        """
        shard_key, messages = batch
        
        # Create a unique filename based on the window end time
        window_end_dt = window.end.to_utc_datetime()
        filename = f"{self.output_path}-{window_end_dt.strftime('%Y%m%d-%H%M%S')}-{shard_key}{self.file_suffix}"

        try:
            with gcsio.GcsIO().open(filename=filename, mode='w') as f:
                for msg in messages:
                    f.write(msg.encode('utf-8') + b'\n')
            logging.info(f"Successfully wrote batch to {filename}")
        except Exception as e:
            logging.error(f"Error writing batch to {filename}: {e}")

# --- The Final Beam Pipeline ---
def run():
    pipeline_options = PipelineOptions(streaming=True)
    gc_options = pipeline_options.view_as(beam.options.pipeline_options.GoogleCloudOptions)
    
    # project_id = gc_options.project
    input_topic = "projects/orbital-bee-462505-v9/topics/stock_data"
    raw_archive_path = "gs://stock-project-data/stock-datalake/"
    dead_letter_path = "gs://stock-project-data/stock-deadletter/"
    bq_table_spec = "orbital-bee-462505-v9.stock_data.stock_trades_clean"

    with beam.Pipeline(options=pipeline_options) as p:
        raw_messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            topic=input_topic
        ).with_output_types(bytes)

        decoded_messages = raw_messages | "Decode" >> beam.Map(lambda x: x.decode("utf-8"))

        # --- Branch 1: Cleaned data to BigQuery (No Windowing, No Change) ---
        processed_results = decoded_messages | "ParseAndClean" >> beam.ParDo(parse_and_clean).with_outputs('dead_letter', main='clean')
        clean_records = processed_results.clean
        (clean_records | "WriteCleanToBigQuery" >> beam.io.WriteToBigQuery(
            table=bq_table_spec,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER
            )
        )

        # --- Branch 2: Raw data to GCS (Using the new robust pattern) ---
        (decoded_messages # Using decoded messages for simplicity in the writer DoFn
            | "WindowRaw" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "AddKeyToRaw" >> beam.Map(lambda x: (random.randint(0, 2), x)) # Add a random key for sharding
            | "GroupRawByKey" >> beam.GroupByKey() # The explicit GroupByKey!
            | "WriteRawBatch" >> beam.ParDo(WriteBatchToFile(raw_archive_path, '.jsonl'))
        )

        # --- Branch 3: Dead-letter data to GCS (Also using the new robust pattern) ---
        dead_letter_records = processed_results.dead_letter
        (dead_letter_records
            | "WindowDead" >> beam.WindowInto(beam.window.FixedWindows(60))
            | "AddKeyToDead" >> beam.Map(lambda x: (0, x)) # Key can be static here
            | "GroupDeadByKey" >> beam.GroupByKey() # The explicit GroupByKey!
            | "WriteDeadBatch" >> beam.ParDo(WriteBatchToFile(dead_letter_path, '.txt'))
        )

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    # The argparse section for command-line arguments would go here if needed,
    # but for simplicity, we are passing them directly in the shell command.
    run()
