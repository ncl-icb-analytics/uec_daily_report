import sys
import pathlib
import os 
from os import getenv
import ncl_sqlsnippets as snips
from datetime import datetime as dtt, timedelta
from dotenv import load_dotenv
import re
import pandas as pd
import numpy as np
from sqlalchemy import types

from utils.smart_api import *
from utils.live_tracker_extract import *
from utils.sandpit_management import *
from utils.global_params import *
from utils.visualisation_functions import *
from utils.network_management import *

### Set which pipelines to run ###
debug_run = {
    "smart_api": True,
    "las_handover": True,
    "live_tracker": True
}

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)
site_id_map = pd.read_csv("./lookups/org_lookup.csv")

### Generate file for intermediate wrangle:

pd.DataFrame([], 
             columns=['source', 'indicatorKeyName', 'site_code_ref', 
                      'ReportDate', 'metric_type', 'value']
             ).to_csv('inter.csv', mode='w', index=False, header=True)

#Line break in terminal
print()

'''
Pull from smart API
'''
if debug_run["smart_api"]:
    print("#########   Processing SMART API Pipeline   #########")

    ### Import settings from the .env file
    env = import_settings(config, "smart_api")

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

    smart_id_map = site_id_map[site_id_map["dataset"] == "smart_api"]

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

            ##Convert API response data into universal format for future work
            inter = processing_data_for_storage(config, res, 
                                                date_start, date_end)

            inter.to_csv('inter.csv', mode='a', index=False, header=False)

            # this needs to be generic enough for all 4 pipelines
            res_for_upload = res.merge(smart_id_map, how="left", 
                                       left_on="siteId", 
                                       right_on="dataset_reference")
            res_for_upload.drop(["dataset", "dataset_reference"], axis=1)

            #Upload and manage datasets
            query_del = get_delete_query(date_start, date_end, [res_for_upload["provider_code"][0]], env)

            upload_request_data(res_for_upload, query_del, env)

            print(f"Upload successful for {site} for ", 
                  f"{date_start} to {date_end}")

    print("\n")  

'''
LAS import
'''
if debug_run["las_handover"]:

    print("#########   Processing LAS Handover Pipeline   #########")

    ### Import settings from the .env file
    env = import_settings(config, "las")

    las_data_dir = getenv("NETWORKED_DATA_PATH_LAS")

    # Load the sheet into a dataframe
    try:
        las_files = fetch_excel_file(las_data_dir, ext=".xlsx")
    except Exception as e:
        print(e)

    for las_file in las_files:

        print(f"Processing: {las_file}")

        las_file_path = path.join(las_data_dir,las_file)
        #Get report date
        ##ASSUMES the date is written in cell I3
        las_data_header = pd.read_excel(las_file_path, sheet_name= "Sheet1", skiprows=1)
        date_data = las_data_header.iloc[0].iloc[8].date()

        #Load the latest data
        las_data = pd.read_excel(las_file_path, sheet_name= "Sheet1", skiprows=5)
        las_data.rename(columns=clean_column_name, inplace=True)
        las_data.columns = map(str.lower, las_data.columns)

        # filter to keep only relevent data
        ## Add site reference codes
        las_id_map = site_id_map[site_id_map["dataset"] == "las"]
        las_data = las_data.merge(las_id_map, how="left", left_on="hospital_site", right_on="dataset_reference")

        ## NCL only
        las_data = las_data.dropna(subset=["provider_code"])

        #Relevant metrics only
        las_data = las_data[["provider_code", "total_handover", "over_15mins", "over_30mins", "over_45mins", "over_60mins", "over_120mins", "over_180mins"]]
        las_data["date_data"] = date_data

        #Upload the data
        query_del = get_delete_query(date_data, date_data, ["RAL01", "RAL26", "RALC7", "RAP", "RKE", "RRV"], env)
        upload_request_data(las_data, query_del, env)
        print(f"Upload successful for las: {las_file}")

        if env["ARCHIVE_LAS"]:
            try:
                archive_data_file(las_file_path, las_data_dir, "las_handover", date_data.strftime("%Y-%m-%d"), ext=".xlsx")
            except FileExistsError:
                print(f"Unable to archive las file as there is already a file with {date_data.strftime("%Y-%m-%d")} data in the archive folder.")

    print("\n")

'''
Live Tracker
'''
if debug_run["live_tracker"]:

    print("#########   Processing Live Tracker Pipeline   #########")

    #Load base settings
    env = import_settings(config, "live_tracker")
    
    #Import the functions for processing the files

    datasets = ["mo", "p2", "vw"]
    new_data_files = scan_new_files(datasets, env)

    #Run the extract function for each dataset
    for ds in datasets:
        print(f"\nProcessing new {ds} data...")

        if new_data_files[ds] == []:
            print_status(404, None)
        else:
            ef_controller(ds, env, new_data_files[ds])

    print("\n")     