import ncl_sqlsnippets as snips
import time
import pyodbc

import pandas as pd
from snowflake.connector.pandas_tools import write_pandas

#Force delay between requests
def add_delay(seconds):
    time.sleep(int(seconds))

# Sandpit data pull

def get_sandpit_data(env, query):
    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
    res = snips.execute_sfw(engine, query)
    return res


# Build the delete query to remove duplicate data
def get_delete_query(date_start, date_end, sites, destination):

    query = f"""
            DELETE FROM {destination}
            WHERE DATE_DATA >= '{date_start}' 
            AND DATE_DATA <= '{date_end}'
            """
    
    if sites:
        sites_string = ""
        for site in sites:
            sites_string += f"'{site}', "
        
        query += f"AND SITE_CODE IN  ({sites_string[:-2]})"
    
    return query

'''
This needs unesting
'''

#Upload the request data
def upload_request_data(data, query_del, env, chunks=100):

    #Delete existing data
    

    #Upload the data
    try:
        #Connect to the database
        engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
        if (snips.table_exists(engine, env["SQL_TABLE"], env["SQL_SCHEMA"])):
            #Delete the existing data
            snips.execute_query(engine, query_del)
        #Upload the new data
        snips.upload_to_sql(data, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=chunks)
    except pyodbc.OperationalError:
        print("Disconnected from the sandpit. Waiting before trying again...")
        #If the connection drops, wait and try again
        add_delay(env["WAIT_COOLOFF"])

        try:
            #Connect to the database
            engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
            if (snips.table_exists(engine, env["SQL_TABLE"], env["SQL_SCHEMA"])):
                #Delete the existing data
                snips.execute_query(engine, query_del)
            #Upload the new data
            snips.upload_to_sql(data, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=chunks)
        except pyodbc.OperationalError as e:
            raise Exception("Connectioned dropped again so cancelling execution")
        except pyodbc.ProgrammingError as e:
            raise Exception (e)
    except pyodbc.ProgrammingError as e:
            raise Exception (e)

def upload_df(ctx, df, destination, replace=False, log=True):

    """
    Function to upload a dataframe to Snowflake.

    inputs:
    - ctx: Snowflake connection object 
    (https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect)
    - df: Dataframe object
    - destination: Full table name of the destination
    (e.g. DATABASE_NAME.SCHEMA_NAME.TABLE_NAME)
    - replace: If True, the destination is TRUNCATED before uploading new data
    (If the upload fails, the truncation is rollbacked)

    output:
    Returns Boolean value if the upload was successful
    """

    df = df.copy()
    df.reset_index(drop=True, inplace=True)
    #Needed to prevent "null" strings in the destination
    df = df.where(pd.notnull(df), None)

    cur = ctx.cursor()
    destination_segs = destination.split(".")
    success = False

    try:
        if replace:
            cur.execute(f"TRUNCATE TABLE {destination}")

        # Upload DataFrame
        success, nchunks, nrows, _ = write_pandas(
            conn=ctx,
            df=df,
            table_name=destination_segs[2],
            schema=destination_segs[1],
            database=destination_segs[0],
            overwrite=False
        )

        if not success:
            raise Exception("Failed to write DataFrame to Snowflake.")

        if log:
            print(f"Uploaded {nrows} rows to {destination}")
    except Exception as e:
        print("Data ingestion failed with error:", e)
        cur.execute("ROLLBACK") #Undoes truncation on upload error

    finally:
        cur.close()
    
    return success

def execute_query(ctx, query):
    cur = ctx.cursor()

    success = False
    try:
        cur.execute(query)
        success = True

    except Exception as e:
        print("SQL failed with this message:", e)
        #Needed to undo editing existing data as not IS_LATEST if upload fails
        cur.execute("ROLLBACK") 

    finally:
        cur.close()

    return success