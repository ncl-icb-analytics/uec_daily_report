import json
import pandas as pd
import requests
import time
import os
from os import getenv
import toml

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import csv

#Library from https://github.com/ncl-icb-analytics/sqlsnippets
#pip install ncl-sqlsnippets
import ncl_sqlsnippets as snips

#Process settings
def import_settings(config):
    return {
        "API_URL": config["smart_api"]["base"]["api_url"],
        "API_KEY": getenv("SMART_API_KEY"),
        "SITES": config["smart_api"]["base"]["sites"],
        
        "DATE_END": config["smart_api"]["date"]["end"],
        "DATE_WINDOW": config["smart_api"]["date"]["window"],
        
        "WAIT_PERIOD": config["smart_api"]["wait"]["period"],
        "WAIT_COOLOFF": config["smart_api"]["wait"]["cooloff"],

        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": config["database"]["sql_database"],
        "SQL_SCHEMA": config["database"]["sql_schema"],
        "SQL_TABLE": config["smart_api"]["base"]["sql_table"],
    }

#Takes the input DATE_END and returns it as a date type variable
def process_date_end(input_date_end):
    #Check for keyword to use current date
    if input_date_end == "today":
        date_end = datetime.now().date()
    else:
        #Check a valid date was used
        try:
            date_end = datetime.strptime(input_date_end, "%Y-%m-%d")
        except:
            raise Exception(f"Unrecognised DATE_END in env file: {input_date_end}")

    return date_end

#Takes the input DATE_WINDOW and returns the date_start as a date type variable
def process_date_window(window, date_end):
    #If a number is given then assume it is in terms of days
    if isinstance(window, int):
        return date_end - timedelta(days=window-1)
    
    #If window is written:
    input_window = window.split(" ")

    #Sanitise input
    if len(input_window) != 2:
        raise Exception(f"The window type {window} is not formatted correctly.")
    
    if input_window[1].endswith('s'):
        input_window[1] = input_window[1][:-1]

    #Process window value to get date_start
    if input_window[1] == "day":
        return date_end - timedelta(days = int(input_window[0]) - 1)
    
    elif input_window[1] == "week":
        return date_end - timedelta(days = (int(input_window[0]) * 7) - 1)
    
    elif input_window[1] == "month":
        return date_end - relativedelta(months = int(input_window[0]))
    
    elif input_window[1] == "year":
        return date_end - relativedelta(years = int(input_window[0]))
    
    else:
        raise Exception(f"The window type {window.split(' ')[1]} is not supported.")

#Calculate array of runs to perform
def calculate_runs(date_start, date_end):
    
    #The number of days between the date_end and date_start
    window_days = (date_end - date_start).days 

    #Array to store run windows
    runs = []
    #Cursors to track date through iteration
    date_cursor_live = date_end
    date_cursor_prev = date_end

    #Iterate to create runs of 7 days
    while window_days > 7:
        #Move cursor back 6 days
        date_cursor_live = date_cursor_prev - timedelta(days=6)
        #Add run to array
        runs.append([
                    datetime.strftime(date_cursor_live, "%Y-%m-%d"), 
                    datetime.strftime(date_cursor_prev, "%Y-%m-%d")
                    ])
        #Update window_days to track when to stop creating runs
        window_days = (date_cursor_live - date_start).days
        #Record where the cursor should start for the next run
        date_cursor_prev = date_cursor_live - timedelta(days=1)
    
    #Add final run for leftover days
    runs.append([
                datetime.strftime(date_start, "%Y-%m-%d"), 
                datetime.strftime(date_cursor_prev, "%Y-%m-%d")
                ])

    return runs

#Make a request to the API
def smart_request (url, key, date_start, date_end, hash_id):
    #Set request URL
    req_url = f"{url}api/sitrep/site/{hash_id}/"

    #Set request parameters
    params = {
        "key":key,
        "date_start": date_start,
        "date_end": date_end
    }

    #Set request headers
    headers = {
        "Content-Type": "application/json"
    }

    #Make request
    response = requests.get(req_url, params=params, headers=headers)

    #Check for response status
    if response.status_code == 200:
        data = json.loads(response.text)
        df = pd.DataFrame(data['OUTPUT'])

        return df

    else:
        # Handle the error
        print(f"Error: {response.status_code}")
        raise Exception (response.text) 

#Force delay between requests
def add_delay(seconds):
    time.sleep(int(seconds))

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

# wrangle the downloaded data
def processing_data_for_storage(config, api_pull, date_start, date_end):
    IndicatorList = config["smart_api"]["base"]['indicator_list']
    #data = pd.DataFrame(api_pull)
    #keep only metrics of interest
    data = api_pull.query('indicatorKeyName in @IndicatorList')
    #derive additional metrics
    data = data[['reportDate', 'siteName','indicatorKeyName','value']].reset_index(drop=True)
    data = data.pivot(index=['siteName', 'reportDate'], columns='indicatorKeyName', values='value')
    data = data.reset_index()
    data['breaches'] = data['breaches'].astype('float', errors='ignore')
    data['no_of_attendances'] = data['no_of_attendances'].astype('float')
    data['performance_4_hour'] = 1-(data['breaches']/data['no_of_attendances'])
    data = data.melt(id_vars = ['siteName','reportDate'])
    #add context and tidy
    data['source'] = 'smart_api'
    data['metric_type'] = 'actual'
    data['date_start'] = date_start
    data['date_end'] = date_end
    data = data[['source', 'indicatorKeyName', 'siteName', 'reportDate', 'date_start', 'date_end','metric_type', 'value']]
    data.to_csv('inter.csv', mode='a', index=False, header=False)


#Upload the request data
def upload_request_data(data, date_start, date_end, site, env):

    #Delete existing data
    query_del = get_delete_query(date_start, date_end, site, env)

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

#Execute runs
def execute_runs(config, runs, env):

    #Get request variables
    url = env["API_URL"]
    key = env["API_KEY"]
    hash_sites = env["SITES"]
    delay = env["WAIT_PERIOD"]
    cooloff = env["WAIT_COOLOFF"]

    #Set  True initially so no delay on first request
    init = True

    #Iterate through runs to get all of the data
    for run in runs:

        #Get dates for the run
        date_start = run[0]
        date_end = run[1]

        #Make a get request per site
        for site in hash_sites:

            #Delay after 1st run to prevent Too Many Requests Error
            if init:
                init = False
            else:
                add_delay(delay)

            try:
                res = smart_request(url, key, date_start, date_end, site)
            except:
                print("Overload so waiting...")
                add_delay(cooloff)
                try:
                    res = smart_request(url, key, date_start, date_end, site)
                except:
                    raise Exception("Failed twice so cancelling execution.")
            print(f"Request fulfilled for site {site} from {date_start} to {date_end}")

            ##Future:
            ##Convert API response data into universal format
            processing_data_for_storage(config, res, date_start, date_end)
            
            #Upload and manage datasets
            upload_request_data(res, date_start, date_end, site, env)




