import io
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from typing import Any
from collections.abc import Iterator
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    urls =[
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-01.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-02.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-03.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-04.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-05.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-06.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-07.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-08.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-09.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-10.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-11.parquet',
        'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-12.parquet',
    ]


    def read_parquets_file(urls: list[str]) -> Iterator[pd.DataFrame]:
        for url in urls:
            yield pd.read_parquet(url)

    dataset = pd.concat(read_parquets_file(urls), ignore_index = True)
    return dataset


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'