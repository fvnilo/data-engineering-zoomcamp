#!/usr/bin/env python
import argparse, os

import pyarrow.parquet as pq

from sqlalchemy import create_engine
from time import time

def download_data(url):
    dataset_filename = os.path.basename(url)

    os.system(f"wget {url} -O {dataset_filename}")

    return dataset_filename

def insert_batch(batch, table_name, engine):
    batch_df = batch.to_pandas()

    batch_df.to_sql(name=table_name, con=engine, if_exists='append')
    

def main(params):
    engine = create_engine(f'postgresql://{params.user}:{params.password}@{params.host}:{params.port}/{params.db}')
    dataset_filename = download_data(params.url)
    file = pq.ParquetFile(dataset_filename)
    
    df = next(file.iter_batches(batch_size=10)).to_pandas()
    df.head(0).to_sql(name=params.table_name, con=engine, if_exists='replace')

    df_iter = file.iter_batches(batch_size=100000)

    t_start = time()
    count = 0

    for batch in df_iter:
        count+=1
        print(f'inserting batch #{count}...')

        b_start = time()
        insert_batch(batch, params.table_name, engine)
        b_end = time()
        
        print(f'inserted! time taken {b_end-b_start:10.3f} seconds.\n')
        
    t_end = time()   
    print(f'Completed! Total time taken was {t_end-t_start:10.3f} seconds for {count} batches.')    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)
