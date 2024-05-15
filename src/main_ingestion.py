import sys
import pathlib
import os 

import ncl_sqlsnippets as snips

from dotenv import load_dotenv

from utils.smart_api import *
from utils.live_tracker_extract import *
from utils.sandpit_management import *

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)

### Generate file for intermediate wrangle:

pd.DataFrame([], 
             columns=['source', 'indicatorKeyName', 'site_code_ref', 
                      'ReportDate', 'metric_type', 'value']
             ).to_csv('inter.csv', mode='w', index=False, header=True)

'''
Pull from smart API
'''

print("API program starting...")

### Import settings from the .env file
env = import_settings(config)

### Process the settings to get the start and end dates & 
### determine how many runs are needed to get all the data
date_end = process_date_end(env["DATE_END"])
date_start = process_date_window(env["DATE_WINDOW"], date_end)
runs = calculate_runs(date_start, date_end)

### Execute the runs on the API

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
        print(f"Request fulfilled for site {site}",
              " from {date_start} to {date_end}")

        ##Convert API response data into universal format for future work
        inter = processing_data_for_storage(config, res, date_start, date_end)

        inter.to_csv('inter.csv', mode='a', index=False, header=False)

        #Upload and manage datasets - Very fixed!!
        query_del = get_delete_query(date_start, date_end, site, env)
        # this needs to be generic enough for all 4 pipelines
        upload_request_data(res, query_del, date_start, date_end, site, env)  

print("All API pulls complete")