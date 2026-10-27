from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

# Set the path to your service account key file
PROJECT_ROOT = Path(__file__).resolve().parent.parent
key_path = PROJECT_ROOT / "dags" / "service_account_key.json"

# Set the credentials using the service account key
credentials = service_account.Credentials.from_service_account_file(
    key_path,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Instantiate the BigQuery client with the credentials
client = bigquery.Client(credentials=credentials)

# Set the ID of the dataset to create
dataset_id = 'youtube'

# Construct a full Dataset object to be send to the API
dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")

# Send the API request to create the dataset
dataset = client.create_dataset(dataset)

# Confirm that the dataset has been created
datasets = client.list_datasets()
for dataset in datasets:
    print(dataset.dataset_id)