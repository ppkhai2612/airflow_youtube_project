from pathlib import Path
from google.oauth2 import service_account
from google.cloud import bigquery

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

# Define the table schema
schema = [
    bigquery.SchemaField('videoId', 'STRING'),
    bigquery.SchemaField('trendingAt', 'TIMESTAMP'),
    bigquery.SchemaField('title', 'STRING'),
    bigquery.SchemaField('publishedAt', 'TIMESTAMP'),
    bigquery.SchemaField('channelId', 'STRING'),
    bigquery.SchemaField('channelTitle', 'STRING'),
    bigquery.SchemaField('description', 'STRING'),
    bigquery.SchemaField('tags', 'STRING'),
    bigquery.SchemaField('category', 'STRING'),
    bigquery.SchemaField('defaultAudioLanguage', 'STRING'),
    bigquery.SchemaField('durationSec', 'INTEGER'),
    bigquery.SchemaField('caption', 'BOOLEAN'),
    bigquery.SchemaField('viewCount', 'INTEGER'),
    bigquery.SchemaField('likeCount', 'INTEGER'),
    bigquery.SchemaField('commentCount', 'INTEGER'),
    bigquery.SchemaField('thumbnailUrl', 'STRING')
]

# Create the table references
dataset_ref = client.dataset('youtube')
table1_ref = dataset_ref.table('trending_videos')
table2_ref = dataset_ref.table('trending_videos_test')

# Define the table objects
table1 = bigquery.Table(table1_ref, schema=schema)
table2 = bigquery.Table(table2_ref, schema=schema)

# Create the tables in BigQuery
client.create_table(table1)
client.create_table(table2)

tables = client.list_tables('youtube')
for table in tables:
    print(table.table_id)