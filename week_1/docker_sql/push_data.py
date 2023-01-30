import argparse
import pandas as pd
import os
from sqlalchemy import create_engine
from time import time



def main(params):
    
    user = params.user
    password = params.user
    host = params.host
    port = params.port
    db = params.db
    url= params.url
    flag_colour = False

    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if 'green' in url:
        flag_colour = True
        csv_name = 'green_'
        table_name = "green_taxi_trips"
    else:
        csv_name = 'yellow_'
        table_name = "yellow_taxi_trips"

    if url.endswith('.csv.gz'):
        csv_name = csv_name + 'output.csv.gz'
    else:
        csv_name = csv_name + 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    #Creating Postgresql connection
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # The dataset is too big, and for that reason there are certain problems loading it
    # To correct this, chunksize parameter can be used (divide datasets in smaller chunks)

    taxi_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(taxi_iter)
    df.head()

    #In some cases, datetime fields can appear as text field. To correct this

    if flag_colour == False:

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    else:
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    #printing SQL possible schema to create
    #print(pd.io.sql.get_schema(df,name="yellow_taxi_data", con=engine))


    # Write the first row (column labels) in the database
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")


    # Append the remaining data in dataset
    df.to_sql(name=table_name, con=engine, if_exists="append")


    ''' The previous sentence added the first chunk only.
        To add the all, you can use a loop
        Atention! A break must be added if you want to avoid an error, 
        for getting out of the loop when all data has been added'''
    print("Initializing data load...")

    while True:

        try:
           
            t_start = time()

            df = next(taxi_iter)

            if flag_colour == False:

                df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
                df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            else:
                df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
                df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)


            df.to_sql(name=table_name, con=engine, if_exists="append")

            t_end = time()


            print('The chunk has been added in %.3f seconds' % (t_end - t_start)  )

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':

    # Argparse

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    # user, password, host, port, database name, table name, CSV url
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--url', help='url of the csv')



    args = parser.parse_args()
    
    main(args)




