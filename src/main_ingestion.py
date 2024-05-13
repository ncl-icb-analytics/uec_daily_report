import sys
import pathlib
import os 
from os import getenv
from datetime import datetime
from dotenv import load_dotenv
import re

from utils.smart_api import *
from utils.sandpit_management import *

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)

### Generate file for intermediate wrangle:
with open('inter.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['source', 'indicatorKeyName', 'siteName', 'ReportDate','date_start', 'date_end', 'metric_type', 'value'])

'''
Pull from smart API
'''

print("API program starting...")

### Import settings from the .env file
env = import_settings(config)

### Process the settings to get the start and end dates & determine how many runs are needed to get all the data
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
        print(f"Request fulfilled for site {site} from {date_start} to {date_end}")

        ##Convert API response data into universal format for future work
        processing_data_for_storage(config, res, date_start, date_end)
        
        #Upload and manage datasets - Broken??
        query_del = get_delete_query(date_start, date_end, site, env)
        upload_request_data(res, query_del, date_start, date_end, site, env)  # this needs to be generic enough for all 4 pipelines

print("All API pulls complete")

'''
LAS import
'''

# Load the sheet into a dataframe
las_file_path = getenv("NETWORKED_DATA_PATH_LAS")
las_data = pd.read_excel(las_file_path, sheet_name= "Data_Ambulance_Handovers")

# clean column names
def clean_column_name(column_name):
    column_name = column_name.strip()
    #column_name = re.sub(r"[^\w\s]", "", column_name)
    column_name = re.sub(r"\n", "", column_name)
    column_name = re.sub(r"\s+", "_", column_name)
    return column_name

las_data.rename(columns=clean_column_name, inplace=True)
las_data.columns = map(str.lower, las_data.columns)

# filter to keep only relevent data
## NCL only
las_data = las_data.query('stp_code == "QMJ"').reset_index(drop = True) # NCL STP only
las_data = las_data.drop(['stp_code', 'stp_short', 'weekday', 'id'], axis=1) # columns not needed 
## Metrics of interest only
las_data = las_data.melt(id_vars = ['hospital_site', 'period'], var_name='indicatorKeyName', value_name='value') # lengthen data to allow filter
IndicatorList = config["las"]["base"]['indicator_list'] # import metric list
las_data = las_data.query('indicatorKeyName in @IndicatorList') # filter metrics list
las_data = las_data[['period', 'hospital_site','indicatorKeyName','value']].reset_index(drop=True) 
## Period of interest only
las_data['period'] = pd.to_datetime(las_data['period'], unit='D', origin='1899-12-30')#, errors='coerce')
las_data['cutoff'] = pd.Timestamp(datetime.now()).date()-pd.to_timedelta(14, unit='d')
las_data = las_data.query("period >= cutoff")

# add to inter

# upload to sandpit - once suficiently generalised
las_data = las_data.pivot(index=['period', 'hospital_site'], columns='indicatorKeyName', values='value')
las_data = las_data.reset_index()
print(las_data)
#query_del = get_delete_query(date_start, date_end, site, env)
#upload_request_data(res, query_del, date_start, date_end, site, env) 
