# Install dependencies
# pip install dlt[bigquery] google-cloud-bigquery-storage google-crc32c

# Example to run the code
# python3 load_data_gcs_to_bigquery.py --type yellow --initial_year 2019 --initial_month 1 --start_year 2019 --end_year 2020 --start_month 2 --end_month 12

import os
import dlt
import argparse
from dlt.sources.filesystem import filesystem, read_parquet
from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Configuration (could be moved to environment variables or config file)
SERVICE_ACCOUNT_FILE = "/home/roman/.gc/ny-taxi-kestra-roman.json"
BUCKET = "kestra-roman1-de-zoomcamp-bucket"
BIGQUERY_DATASET = "de_zoomcamp_dataset"
PROJECT_ID = "ny-taxi-449605"
LOG_FREQUENCY = 500000  # Log every 500k rows

def create_bigquery_dataset():
    """Create BigQuery dataset if it doesn't exist"""
    client = bigquery.Client()
    dataset_id = f"{PROJECT_ID}.{BIGQUERY_DATASET}"
    
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists")
    except NotFound:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Dataset {dataset_id} created")

def load_credentials():
    """Load Google Cloud credentials"""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise FileNotFoundError(f"Service account file '{SERVICE_ACCOUNT_FILE}' not found")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SERVICE_ACCOUNT_FILE
    print("Google Cloud credentials loaded")

def generate_date_ranges(start_year, end_year, start_month, end_month):
    """Generate (year, month) tuples for given date range"""
    for year in range(start_year, end_year + 1):
        s_month = start_month if year == start_year else 1
        e_month = end_month if year == end_year else 12
        for month in range(s_month, e_month + 1):
            yield year, month

def create_parquet_resource(args, file_glob, write_disposition):
    """Create a dlt resource for processing parquet files"""
    @dlt.resource(
        name=f"{args.type}_taxi",
        write_disposition=write_disposition
    )
    def parquet_reader():
        bucket_url = f"gs://{BUCKET}/{args.type}/"
        files = filesystem(bucket_url, file_glob=file_glob)
        data = files | read_parquet()
        
        row_count = 0
        for row in data:
            row_count += 1
            if row_count % LOG_FREQUENCY == 0:
                print(f"Processed {row_count} rows...")
            yield row
        print(f"Total rows processed: {row_count}")

    return parquet_reader

def main():
    # Load credentials and initialize BigQuery
    load_credentials()
    create_bigquery_dataset()

    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Load NYC Taxi data into BigQuery")
    parser.add_argument("--type", choices=["green", "yellow", "fhv"], required=True)
    parser.add_argument("--initial_year", type=int, default=2019)
    parser.add_argument("--initial_month", type=int, default=1)
    parser.add_argument("--start_year", type=int, default=2019)
    parser.add_argument("--end_year", type=int, default=2020)
    parser.add_argument("--start_month", type=int, default=2)
    parser.add_argument("--end_month", type=int, default=12)
    args = parser.parse_args()

    # Initialize pipeline
    pipeline = dlt.pipeline(
        pipeline_name=f"{args.type}_taxi_pipeline",
        dataset_name=BIGQUERY_DATASET,
        destination="bigquery"
    )

    # Initial load with replacement
    initial_file = f"{args.type}_tripdata_{args.initial_year}-{args.initial_month:02d}.parquet"
    initial_resource = create_parquet_resource(args, initial_file, "replace")
    print(f"\n{'='*40}\nLoading initial data: {initial_file}\n{'='*40}")
    pipeline.run(initial_resource)

    # Append subsequent months
    print(f"\n{'='*40}\nAppending additional data\n{'='*40}")
    for year, month in generate_date_ranges(args.start_year, args.end_year, args.start_month, args.end_month):
        if year == args.initial_year and month == args.initial_month:
            continue  # Skip initial month
        
        file_pattern = f"{args.type}_tripdata_{year}-{month:02d}.parquet"
        append_resource = create_parquet_resource(args, file_pattern, "append")
        
        print(f"\nProcessing {file_pattern}...")
        try:
            info = pipeline.run(append_resource)
            print(f"Successfully processed {file_pattern}:\n{info}")
        except Exception as e:
            print(f"Error processing {file_pattern}: {str(e)}")
            raise

if __name__ == "__main__":
    main()
