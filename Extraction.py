from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.engine import URL
import pandas as pd
import io
import boto3
import pyodbc
import snowflake.connector
import yaml

with open('C:/Users/tesse/OneDrive/Desktop/ETL Demo/New_Batch_Assesement_repo/cred.yaml', 'r') as file:
    credentials = yaml.safe_load(file)

# GET API KEYS
access_key = credentials['access_key']
secret_access_key = credentials['secret_access_key']

# FOLDER CREATION IN S3 BUCKET
bucket_name = credentials['bucket_name']
server = credentials['server']
now = datetime.now()
day = now.strftime("%d%m%Y")

# CONNECTING TO AWS S3
s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,
                         region_name='ap-south-1')



# EXTRACTION
def extract_data(database_name, table_name, batch_size):
    start_time = datetime.now().strftime('%F %T.%f')[:-3]
    print(start_time)

    sf_conn = snowflake.connector.connect(
        user=credentials['user'],
        password=credentials['password'],
        account=credentials['account'],
        warehouse=credentials['warehouse'],
        database=credentials['database'],
        schema=credentials['schema'],
        role=credentials['role']
    )

    tables = credentials['tables']
    print(tables)

    if table_name=='all_tables':
        tables=tables
    else:
        tables=[]
        tables.append(table_name)

    for table_name in tables:
        try:
            cs = sf_conn.cursor()
            cs.execute(
                "insert into JOB_STATUS_TABLE(JOB_NAME,START_TIME) values('{}','{}')".format(
                    table_name, start_time))

            fetch_status = cs.execute(
                "select max(start_time) from job_status_table where JOB_NAME = '{}' and job_status = 'SUCCESS'"
                .format(table_name))
            last_l = pd.DataFrame(fetch_status)
            if last_l[0][0] == None or 0:
                last_load = '1900-01-01 00:00:00.000'
                print(last_load)
            else:
                row2 = last_l.iloc[0][0]
                a = str(row2)
                last_load = a[:23]
                print(last_load)


            #SQL SERVER DETAILS
            conn_str = "DRIVER={SQL Server};PORT=1433;SERVER=%s;DATABASE=%s;Trusted_Connection=yes;" % (
            server, database_name)
            cnxn = pyodbc.connect(conn_str)
            connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": conn_str})
            cursor = cnxn.cursor()
            engine = create_engine(connection_url)
            session = scoped_session(sessionmaker(bind=engine))
            s = session()


            incr = "{}/{}/".format(table_name, day)
            # Selecting data from SQL Server
            cursor.execute("select count(updated_on) from {} where updated_on > '{}'".format(table_name, last_load))
            count = cursor.fetchone()[0]
            print(count)
            # if count==0:
            #     cs.execute(
            #         "delete from JOB_STATUS_TABLE where JOB_NAME = '{}' and START_TIME= '{}'".format(
            #         table_name, start_time))
            if count % batch_size == 0:
                no_of_batches = count // batch_size
            else:
                no_of_batches = (count // batch_size) + 1
            print(no_of_batches)
            offset = 0
            for i in range(no_of_batches):
                fetch_data = (
                    "SELECT * FROM {} where updated_on > '{}' ORDER BY updated_on OFFSET {} ROWS FETCH NEXT {} rows ONLY").format(
                    table_name, last_load, offset, batch_size)
                df = pd.read_sql_query(text(fetch_data), con=engine.connect())
                print(df)

                offset = offset + batch_size
                print(offset)
                if df.empty:
                    print('No Latest Records')
                    break
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
            "update JOB_STATUS_TABLE set END_TIME='{}' ,Job_status= 'SUCCESS' ,No_of_rows = {} where JOB_NAME = '{}' and start_time= '{}'".format(
                end_time,count, table_name, start_time))
            print('job done')

        except:

            cs.execute(
                "insert into JOB_STATUS_TABLE(JOB_NAME,START_TIME,JOB_STATUS) values('{}','{}', 'FAILED')".format(
                    table_name, start_time))
            print('job Failed')



extract_data('Exceliq', 'all_tables', 500)
