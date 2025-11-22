"""
Kafka Producer for CSV Streaming Data
STUDENT PROJECT: Big Data Streaming Data Producer

This version reads the AIR_QUALITY_PHIL.csv file and streams its content to Kafka.
"""

import argparse
import json
import time
import pandas as pd # <-- NEW: Import Pandas
from datetime import datetime
from typing import Dict, Any, List, Iterator, Optional

# Kafka libraries
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# --- Global Configuration for your Air Quality CSV ---
# List of all 12 metric columns present in your CSV
AIR_QUALITY_METRICS: List[str] = [
    "pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide", 
    "nitrogen_dioxide", "sulphur_dioxide", "ozone", "methane", 
    "uv_index_clear_sky", "uv_index", "dust", "aerosol_optical_depth"
]
# The column in your CSV that holds the timestamp
CSV_TIMESTAMP_COLUMN = 'date' 
# The key the consumer expects for the timestamp
KAFKA_TIMESTAMP_KEY = 'timestamp' 


class StreamingDataProducer:
    """
    Kafka producer adapted to read and stream data from a CSV file.
    """
    
    def __init__(self, bootstrap_servers: str, topic: str, csv_file: str):
        """
        Initialize Kafka producer and load the CSV data.
        """
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.csv_file = csv_file
        self.data_iterator: Optional[Iterator[Dict[str, Any]]] = None # Iterator for the data stream
        self.total_records = 0
        
        # Kafka producer configuration
        self.producer_config = {
            'bootstrap_servers': bootstrap_servers,
            # We will use the 'value_serializer' later when sending the message.
        }
        
        # Load and Prepare CSV Data
        self._load_csv_data()
        
        # Initialize Kafka producer
        try:
            # We set the JSON serializer directly here
            self.producer = KafkaProducer(
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                **self.producer_config
            )
            print(f"✅ Kafka producer initialized for {bootstrap_servers} on topic {topic}")
        except NoBrokersAvailable:
            print(f"❌ ERROR: No Kafka brokers available at {bootstrap_servers}")
            self.producer = None
        except Exception as e:
            print(f"❌ ERROR: Failed to initialize Kafka producer: {e}")
            self.producer = None

    def _load_csv_data(self):
        """Loads CSV data and creates an iterator for streaming."""
        try:
            # Load CSV data into a Pandas DataFrame
            df = pd.read_csv(self.csv_file)
            self.total_records = len(df)
            
            # Ensure the DataFrame is sorted by the date column
            if CSV_TIMESTAMP_COLUMN in df.columns:
                df = df.sort_values(by=CSV_TIMESTAMP_COLUMN).reset_index(drop=True)
            else:
                print(f"❌ ERROR: CSV missing required column: '{CSV_TIMESTAMP_COLUMN}'")
                return

            # Add default metadata if missing (assuming a single sensor/location for this file)
            if 'sensor_id' not in df.columns:
                 df['sensor_id'] = 'AQ_STATION_PHIL'
            if 'location' not in df.columns:
                 df['location'] = 'Manila, Philippines'
            if 'unit' not in df.columns:
                 df['unit'] = 'µg/m³' # Micrograms per cubic meter (a common AQ unit)

            print(f"📄 Loaded {self.total_records} records from {self.csv_file}")
            
            # Convert DataFrame rows to dictionaries and create an iterator
            # The iterator yields one dictionary (row) at a time
            self.data_iterator = df.to_dict('records').__iter__()
            
        except FileNotFoundError:
            print(f"❌ ERROR: CSV file '{self.csv_file}' not found.")
        except Exception as e:
            print(f"❌ ERROR reading CSV: {e}")

    # DEPRECATED: We are replacing this with generate_csv_data
    # def generate_sample_data(self) -> Dict[str, Any]:
    #     # This function is removed or left empty as we use CSV data
    #     pass 

    def generate_csv_data(self) -> Optional[Dict[str, Any]]:
        """
        Reads the next row from the CSV data iterator and bundles it into
        the wide JSON format required by the consumer app.
        
        Returns:
        - Dict[str, Any]: The bundled message payload, or None if the stream is finished.
        """
        try:
            # Get the next row dictionary from the iterator
            row: Dict[str, Any] = next(self.data_iterator)
            
            # --- CRITICAL REVISION: Mapping and Bundling ---
            
            # Map the CSV's 'date' column to the Kafka message's 'timestamp' key
            #timestamp_value = row.get(CSV_TIMESTAMP_COLUMN)
            # alternative to make it real simulation


            """
            Since we cant use the API live call
            We just maximize our calls and merge all the data sets in one
            Now every time that the producer produces data from the csv
            we implment the real time just to make the simulation looks real
            """
            timestamp_value = datetime.now().isoformat()
            
            if timestamp_value is None:
                print(f"⚠️ Warning: Missing {CSV_TIMESTAMP_COLUMN} in row. Skipping.")
                return None
            
            # The message payload is a single, "wide" record containing all metrics
            message_payload = {
                KAFKA_TIMESTAMP_KEY: str(timestamp_value), 
                'sensor_id': str(row.get('sensor_id')),
                'location': str(row.get('location')),
            }
            
            # Add all air quality metrics
            for metric in AIR_QUALITY_METRICS:
                value = row.get(metric)
                # Check for Pandas NaN (represented as float('nan') in dict) or None
                if pd.isna(value) if isinstance(value, (float, int)) else value is None:
                    message_payload[metric] = None # Send None for missing values
                else:
                    message_payload[metric] = float(value)
            
            return message_payload

        except StopIteration:
            # This exception is raised when the iterator runs out of data (end of CSV)
            return None
        except Exception as e:
            print(f"⚠️ Error processing row: {e}")
            return None

    def serialize_data(self, data: Dict[str, Any]) -> bytes:
        """
        Convert the data dictionary to bytes for Kafka transmission (JSON).
        """
        try:
            # JSON serialization (used by KafkaProducer's value_serializer in __init__)
            # This method now just returns the JSON serialized data
            serialized_data = json.dumps(data).encode('utf-8')
            return serialized_data
        except Exception as e:
            print(f"ERROR: Serialization failed: {e}")
            return None

    def send_message(self, data: Dict[str, Any]) -> bool:
        """
        Implement message sending to Kafka (unchanged from template).
        """
        if not self.producer:
            print("ERROR: Kafka producer not initialized")
            return False
        
        # Serialize the data (though it's technically serialized by the KafkaProducer init)
        # We ensure the data is formatted correctly before sending.
        
        try:
            # Send message to Kafka (serialization handled by value_serializer)
            future = self.producer.send(self.topic, value=data) 
            future.get(timeout=10) # Wait for send confirmation
            return True
            
        except KafkaError as e:
            print(f"Kafka send error for message: {data[KAFKA_TIMESTAMP_KEY][:19]}... | {e}")
            return False
        except Exception as e:
            print(f"Unexpected error during send: {e}")
            return False

    def produce_stream(self, messages_per_second: float = 0.1, duration: int = None):
        """
        Main streaming loop: Iterates through the CSV data until finished.
        
        The 'duration' argument is ignored as streaming is controlled by the CSV length.
        """
        if not self.data_iterator:
            print("❌ Cannot start stream: CSV data failed to load or is empty.")
            return

        print(f"Starting CSV stream: {messages_per_second} msg/sec ({1/messages_per_second:.1f} second intervals) over {self.total_records} records.")
        
        start_time = time.time()
        message_count = 0
        
        # Calculate sleep time to maintain desired message rate
        sleep_time = (1.0 / messages_per_second) if messages_per_second > 0 else 0
        
        try:
            while True:
                # ----------------------------------------------------
                # STUDENT TODO: Use CSV iteration logic here
                data = self.generate_csv_data()
                # ----------------------------------------------------
                
                if data is None:
                    print("✅ End of CSV data reached.")
                    break
                
                # Send data and log success/failure
                success = self.send_message(data)
                
                if success:
                    message_count += 1
                    # Log message progress (print less often for high rates)
                    # lets put an indicator to check if ok pa yung producer HAHAHAHA
                    if message_count % 10 == 0 or messages_per_second < 1:
                         print(f"⬆️ Sent {message_count}/{self.total_records} records. TS: {data[KAFKA_TIMESTAMP_KEY]}")
                
                # Apply rate limiting
                if sleep_time > 0:
                    time.sleep(sleep_time)
                
        except KeyboardInterrupt:
            print("\nProducer interrupted by user")
        except Exception as e:
            print(f"Streaming error: {e}")
        finally:
            self.close()
            print(f"Producer stopped. Total messages sent: {message_count}")

    def close(self):
        """Implement producer cleanup and resource release (unchanged from template)."""
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
                print("Kafka producer closed successfully")
            except Exception as e:
                print(f"Error closing Kafka producer: {e}")


def parse_arguments():
    """Configures command-line arguments, including the new --csv argument."""
    parser = argparse.ArgumentParser(description='Kafka Streaming Data Producer (CSV Mode)')
    
    parser.add_argument(
        '--bootstrap-servers',
        type=str,
        default='localhost:9092',
        help='Kafka bootstrap servers (default: localhost:9092)'
    )
    
    parser.add_argument(
        '--topic',
        type=str,
        default='streaming-data',
        help='Kafka topic to produce to (default: streaming-data)'
    )
    
    parser.add_argument(
        '--rate',
        type=float,
        default=1.0, # Changed default rate to 1 message/second
        help='Messages per second (default: 1.0)'
    )
    
    # NEW: Argument for CSV file
    parser.add_argument(
        '--csv',
        type=str,
        required=True, # Make CSV file mandatory
        help='Path to the input CSV file (e.g., AIR_QUALITY_PHIL.csv)'
    )
    
    # Duration is now ignored for CSV mode but kept for template integrity
    parser.add_argument(
        '--duration',
        type=int,
        default=None,
        help='(IGNORED IN CSV MODE) Run duration in seconds.'
    )
    
    return parser.parse_args()


def main():
    """Main execution flow for CSV producer."""
    
    print("=" * 60)
    print("AIR QUALITY CSV STREAMING DATA PRODUCER")
    print("=" * 60)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Initialize producer (passing the CSV file path)
    producer = StreamingDataProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        csv_file=args.csv # Pass CSV file to the producer class
    )
    
    # Start producing stream
    try:
        producer.produce_stream(
            messages_per_second=args.rate
        )
    except Exception as e:
        print(f"Main execution error: {e}")
    finally:
        print("Producer execution completed")


if __name__ == "__main__":
    main()