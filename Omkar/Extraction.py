# FINAL CODE WITH BATCH PROCESSING


from datetime import date
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import URL
import pandas as pd
import json
import io
import boto3
import os
import pyodbc

# GET API KEYS
content = open('aws_credmt.json')
config = json.load(content)
access_key = config['access_key']
secret_access_key = config['secret_access_key']

now = datetime.now()
day = now.strftime("%d%m%Y")

# FOLDER CREATION IN S3 BUCKET
BUCKET_NAME = "om-test-sql"
ts_col = "currentdate"
incr = "etl_project/incr/"
hist = "etl_project/hist/"
full_load = "etl_project/full_load/batch/"
server = "DESKTOP-I2P6GLV\SQLEXPRESS"

# CONNECTING TO AWS S3
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                         region_name='ap-south-1')


# EXTRACTION
def extract(database_name, load_type, source_table):
    # SQL SERVER DETAILS
    conn_str = "DRIVER={SQL Server};PORT=1433;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;" % (server, database_name)

    # cnxn = pyodbc.connect(conn_str)
    connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})

    engine = create_engine(connection_url)
    session = scoped_session(sessionmaker(bind=engine))
    s = session()

    # LOADING INCR DATA FOR SPECIFIC TABLE
    if load_type == "incr_load":

        # FETCHING INCR RECORDS OF ALL TABLE
        src_tables = s.execute("""select name as table_name from sys.tables""")
        print("src_tabls", src_tables)
        if source_table == 'all_tables':
            for tbl in src_tables:
                print("tbl", tbl)
                fetch_incr_at = "select * from {} where convert(varchar(10),currentdate,111) = (SELECT MAX(CONVERT(VARCHAR(10),currentdate,111)) FROM {})".format(
                    tbl[0], tbl[0])
                print(fetch_incr_at)
                fia = pd.read_sql_query(fetch_incr_at, engine)
                print("running till here:", fia)

                with io.StringIO() as csv_buffer:
                    # LOADING INTO INCR FOLDER
                    fia.to_csv(csv_buffer, index=False)
                    response = s3_client.put_object(Bucket=BUCKET_NAME, Key=incr + tbl[0] + "/" + tbl[0] + ".csv",
                                                    Body=csv_buffer.getvalue())

                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status == 200:
                        print(f"Successful S3 put_object response. Status - {status}")
                    else:
                        print(f"Unsuccessful S3 put_object response. Status - {status}")
                    print("Data Imported Successful")

                    # LOADING INTO HISTORICAL FOLDER
                    fia.to_csv(csv_buffer, index=False, mode='a')
                    response = s3_client.put_object(Bucket=BUCKET_NAME,
                                                    Key=hist + tbl[0] + "/" + day + "/" + tbl[0] + ".csv",
                                                    Body=csv_buffer.getvalue())
                    status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                    if status == 200:
                        print(f"Successful S3 put_object response. Status - {status}")

        else:
            for tbl in src_tables:
                if tbl[0] == source_table:
                    # FETCHING INCR RECORDS OF SPECIFIC TABLE
                    fetch_latest_record = "select * from {} where convert(varchar(10),currentdate,111) = (SELECT MAX(CONVERT(VARCHAR(10),currentdate,111)) FROM {})".format(
                        tbl[0], tbl[0])
                    print(fetch_latest_record)
                    flr = pd.read_sql_query(fetch_latest_record, con=engine)
                    print(flr, source_table)

                    with io.StringIO() as csv_buffer:
                        # LOADING INTO INCR FOLDER
                        flr.to_csv(csv_buffer, index=False)
                        response = s3_client.put_object(Bucket=BUCKET_NAME,
                                                        Key=incr + source_table + "/" + source_table + ".csv",
                                                        Body=csv_buffer.getvalue())
                        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                        if status == 200:
                            print(f"Successful S3 put_object response. Status - {status}")
                        else:
                            print(f"Unsuccessful S3 put_object response. Status - {status}")
                        print("Data Imported Successful")

                        # LOADING INTO HISTORICAL FOLDER
                        flr.to_csv(csv_buffer, index=False, mode='append')
                        response = s3_client.put_object(Bucket=BUCKET_NAME,
                                                        Key=hist + source_table + "/" + day + "/" + source_table + ".csv",
                                                        Body=csv_buffer.getvalue())
                        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                        if status == 200:
                            print(f"Successful S3 put_object response. Status - {status}")

    elif load_type == "full_load":

        # FETCHING FULL LOAD FOR ALL TABLES
        src_tables = s.execute("""select name as table_name from sys.tables""")
        print(src_tables)
        if source_table == 'all_tables':
            batch_size = 100
            for tbl in src_tables:
                fetch_full_load = "select * from {}".format(tbl[0])
                print(fetch_full_load)
                df = pd.read_sql_query(fetch_full_load, engine)
                for i in range(0, len(df), batch_size):
                    batch = df[i:i + batch_size]



                    with io.StringIO() as csv_buffer:
                        # LOADING INTO ALL TABLES
                        batch.to_csv(csv_buffer, index=False)
                        response = s3_client.put_object(Bucket=BUCKET_NAME,
                                                        Key=full_load + tbl[0] + "/" + tbl[0] + str(datetime.now()) + ".csv",
                                                        Body=csv_buffer.getvalue())

                        status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                        if status == 200:
                            print(f"Successful S3 put_object response. Status - {status}")
                        else:
                            print(f"Unsuccessful S3 put_object response. Status - {status}")
                        print("Data Imported Successful")

        # SELECTING SPECIFIC TABLES FOR FULL LOAD
        else:
            batch_size = 100
            for tbl in src_tables:
                if tbl[0] == source_table:
                    fetch_spec_load = "select * from {}".format(tbl[0])
                    print(fetch_spec_load)
                    df = pd.read_sql_query(fetch_spec_load, engine)
                    for i in range(0, len(df), batch_size):
                        batch = df[i:i + batch_size]
                        print(df)

                        with io.StringIO() as csv_buffer:
                            # LOADING FOR SPECFIC TABLES
                            batch.to_csv(csv_buffer, index=False)
                            response = s3_client.put_object(Bucket=BUCKET_NAME,
                                                            Key=full_load + source_table + "/" + source_table + str(datetime.now()) +".csv",
                                                            Body=csv_buffer.getvalue())

                            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                            if status == 200:
                                print(f"Successful S3 put_object response. Status - {status}")
                            else:
                                print(f"Unsuccessful S3 put_object response. Status - {status}")
                            print("Data Imported Successful")


extract('ABDB', 'incr_load', 'customers')
# def extract(database_name,load_type,source_table='all_tables'):


# full_load - all_tables + full_Load - working
#             spec_table + full_load - working

# incr_load - all_tables + incr  - working
#             spec_table + incr  - working