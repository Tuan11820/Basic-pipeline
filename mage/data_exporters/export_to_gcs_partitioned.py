import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "~/secret_key.json"
project_id = 'homework2-436406'
bucket_name = 'green_taxi_2022'
table_name = 'green_taxi'
root_path = f'{bucket_name}/{table_name}'

@data_exporter
def export_data_to_google_cloud_storage(df: pd.DataFrame, **kwargs) -> None:  # Updated DataFrame definition
    # Convert the pandas DataFrame to a pyarrow Table
    table = pa.Table.from_pandas(df)
    
    # Set up the Google Cloud Storage file system
    gcs = pa.fs.GcsFileSystem()

    # Write the table to a Parquet dataset with partitioning
    pq.write_to_dataset(
        table,
        root_path=root_path,
        partition_cols=['lpep_pickup_date'],  # Assuming this column exists
        filesystem=gcs
    )
