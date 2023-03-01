from datetime import date
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import URL
import pandas as pd
import io
import boto3
import os
import pyodbc
import snowflake.connector
import yaml

# GET API KEYS
access_key = os.environ.get('access_key')
secret_access_key = os.environ.get('secret_access_key')

# FOLDER CREATION IN S3 BUCKET
bucket_name = "etl-snfk1"
# ts_col = "currentdate"
server = "DESKTOP-IND"
now = datetime.now()
day = now.strftime("%d%m%Y")

# CONNECTING TO AWS S3
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                         region_name='ap-south-1')



# EXTRACTION
def extract_data(database_name, table_name, batch_size):
    start_time = datetime.now().strftime('%F %T.%f')[:-3]
    print(start_time)

    with open('cred.yaml', 'r') as file:
        credentials = yaml.safe_load(file)

    sf_conn = snowflake.connector.connect(
        user=credentials['user'],
        password=credentials['password'],
        account=credentials['account'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema'],
        role=credentials['role']
    )
    cs = sf_conn.cursor()
    bucket_name = credentials['bucket_name']
    server = credentials['server']


    try:

        try:
            fetch_status = cs.execute(
                "select max(start_time) from job_status_table where JOB_NAME = '{}' and job_status = 'SUCCESS'"
                .format(table_name))
            last_l = pd.DataFrame(fetch_status)
            row2 = last_l.iloc[0][0]
            a = str(row2)
            last_load = a[:23]
            print(last_load)
        except:
            # Default Last value
            last_load = '1900-01-01 00:00:00.000'
            print(last_load)

        # SQL SERVER DETAILS
        conn_str = "DRIVER={SQL Server};PORT=1433;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;" % (
        server, database_name)
        cnxn = pyodbc.connect(conn_str)
        connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
        cursor = cnxn.cursor()
        engine = create_engine(connection_url)
        session = scoped_session(sessionmaker(bind=engine))
        s = session()

        # FUNCTION ESTABLISHMENT
        # table_name = 'all_tables' OR 'Specific_table_name'
        if table_name == 'all_tables':
            db_tables = [table.table_name for table in cursor.tables(tableType='TABLE')]
            # cursor.close()
            print(db_tables)
        else:
            db_tables = [table_name]
            print(table_name + " table selected.")

        for tbl in db_tables:
            if tbl == 'trace_xe_action_map':
                continue
            if tbl == 'trace_xe_event_map':
                continue
            else:
                incr = "{}/{}/".format(table_name, day)
                # Selecting data from SQL Server
                fetch_load = "select * from {} where updated_on > '{}'".format(table_name, last_load)
                df = pd.read_sql_query(text(fetch_load),con=engine.connect())
                count=len(df)
                print(count)
                if count % batch_size == 0:
                    no_of_batches = count // batch_size
                else:
                    no_of_batches = (count // batch_size) + 1
                print(no_of_batches)
                offset = 0
                for i in range(no_of_batches):
                    fetch_data = (
                        "SELECT * FROM {} where updated_on > '{}' ORDER BY updated_on OFFSET {} ROWS FETCH NEXT {} rows ONLY").format(
                        table_name,last_load,offset, batch_size)
                    df = pd.read_sql_query(text(fetch_data), con=engine.connect())
                    print(df)

                    offset=offset+batch_size
                    print(offset)
                    if df.empty:
                        print('No Latest Records')
                        continue
                    else:
                        with io.StringIO() as csv_buffer:
                            # LOADING INTO INCR FOLDER
                            df.to_csv(csv_buffer, index=False)
                            response = s3_client.put_object(Bucket=bucket_name,
                                                            Key=incr + str(
                                                                datetime.now()) + ".csv",
                                                            Body=csv_buffer.getvalue())
                            print('Data loaded successfully')

                end_time = datetime.now().strftime('%F %T.%f')[:-3]
                cs.execute(
                    "insert into JOB_STATUS_TABLE(JOB_NAME,START_TIME,END_TIME,JOB_STATUS) values('{}','{}', '{}', 'SUCCESS')".format(
                        table_name, start_time, end_time))


    except:

        cs.execute(
            "insert into JOB_STATUS_TABLE(JOB_NAME,START_TIME,JOB_STATUS) values('{}','{}', 'FAILED')".format(
                table_name, start_time))

        print('Job failed')



extract_data('Exceliq', 'customers', 50)
