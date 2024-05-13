

#Build the delete query to remove duplicate data
def get_delete_query(date_start, date_end, site, env):

    sql_database =  env["SQL_DATABASE"]
    sql_schema =  env["SQL_SCHEMA"]
    sql_table =  env["SQL_TABLE"]

    query = f"""
                DELETE FROM [{sql_database}].[{sql_schema}].[{sql_table}] 
                WHERE reportDate >= '{date_start}' 
                    AND reportDate <= '{date_end}' 
                    AND siteId = '{site}'
                """
    
    return query

'''
This needs unesting
'''

#Upload the request data
def upload_request_data(data, query_del, date_start, date_end, site, env):

    #Delete existing data
    

    #Upload the data
    try:
        #Connect to the database
        engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])
        if (snips.table_exists(engine, env["SQL_TABLE"], env["SQL_SCHEMA"])):
            #Delete the existing data
            snips.execute_query(engine, query_del)
        #Upload the new data
        snips.upload_to_sql(data, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=150)
    except:
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
            snips.upload_to_sql(data, engine, env["SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=150)
        except:
            raise Exception("Connectioned dropped again so cancelling execution")

    print(f"Upload successful for site {site} from {date_start} to {date_end}")