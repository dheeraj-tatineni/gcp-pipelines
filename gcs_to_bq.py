import functions_framework
from google.cloud import bigquery
from google.cloud import storage
import os

@functions_framework.cloud_event
def load_gcs_to_bq(cloud_event):
    """
    Cloud Function triggered by GCS finalize event to load data into BigQuery.
    """
    data = cloud_event.data

    bucket_name = data['bucket']
    file_name = data['name']

    # Environment variables for configuration
    dataset_id = os.environ.get('BQ_DATASET')
    table_id = os.environ.get('BQ_TABLE')
    project_id = os.environ.get('GCP_PROJECT')

    if not all([dataset_id, table_id, project_id]):
        raise ValueError("Missing required environment variables: BQ_DATASET, BQ_TABLE, GCP_PROJECT")

    # Initialize clients
    storage_client = storage.Client(project=project_id)
    bq_client = bigquery.Client(project=project_id)

    # Construct BigQuery table reference
    table_ref = bq_client.dataset(dataset_id).table(table_id)

    # Configure the load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Assuming CSV files; adjust as needed
        autodetect=True,  # Automatically detect schema
        skip_leading_rows=1,  # Skip header row; set to 0 if no header
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to existing table
    )

    # GCS URI
    uri = f"gs://{bucket_name}/{file_name}"

    # Start the load job
    load_job = bq_client.load_table_from_uri(uri, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    print(f"Successfully loaded {file_name} from gs://{bucket_name} to {project_id}.{dataset_id}.{table_id}")

    return f"Loaded {file_name} to BigQuery"
