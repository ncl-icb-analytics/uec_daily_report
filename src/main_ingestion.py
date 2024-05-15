import sys
import pathlib
import os 
from os import getenv
import ncl_sqlsnippets as snips
from datetime import datetime, timedelta
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

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)
site_id_map = pd.read_csv("./lookups/org_lookup_smart.csv")


### Generate file for intermediate wrangle:

pd.DataFrame([], 
             columns=['source', 'indicatorKeyName', 'site_code_ref', 
                      'ReportDate', 'metric_type', 'value']
             ).to_csv('inter.csv', mode='w', index=False, header=True)

'''
Pull from smart API
'''
if True:
    print("API program starting...")

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

            #Upload and manage datasets
            query_del = get_delete_query(date_start, date_end, [site], env)
            # this needs to be generic enough for all 4 pipelines
            res_for_upload = res.merge(site_id_map, how="left", 
                                       left_on="siteId", 
                                       right_on="dataset_reference")
            res_for_upload.drop(["dataset", "dataset_reference"], axis=1)

            upload_request_data(res_for_upload, query_del, env)

            print(f"Upload successful for {site} for ", 
                  f"{date_start} to {date_end}")  

    print("All SMART API pulls complete")

'''
LAS import
'''
if True:

    ### Import settings from the .env file
    env = import_settings(config, "las")

    # Load the sheet into a dataframe
    las_file_path = getenv("NETWORKED_DATA_PATH_LAS")
    las_data = pd.read_excel(las_file_path, sheet_name= "Data_Ambulance_Handovers")

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
    las_data['cutoff'] = pd.Timestamp(datetime.datetime.now()).date()-pd.to_timedelta(14, unit='d')
    las_data = las_data.query("period >= cutoff")
    ## Add site reference codes
    las_id_map = site_id_map[site_id_map["dataset"] == "las"]
    las_data = las_data.merge(site_id_map, how="left", left_on="hospital_site", right_on="dataset_reference")

    ## add to inter for graphing
    las_data['source'] = 'las'
    las_data['metric_type'] = 'actual'
    las_data.rename(columns={'period': 'reportDate'}, inplace=True)
    las_data = las_data[['source', 'indicatorKeyName', 'provider_code', 'reportDate', 'metric_type', 'value']]
    las_data.to_csv('inter.csv', mode='a', index=False, header=False)

    ## Sandpit upload
    # reshape for sandpit
    las_data = las_data.pivot(index=['reportDate', 'provider_code'], columns='indicatorKeyName', values='value').reset_index()
    las_data = las_data.rename_axis(None, axis = 1)

    date_end = las_data.max().loc[0].date()
    date_start = (date_end - timedelta(days=14))

    # upload to sandpit - once suficiently generalised
    query_del = get_delete_query(date_start, date_end, ["RAL01", "RAL26", "RALC7", "RAP", "RKE", "RRV"], env)
    upload_request_data(las_data, query_del, env)
    print(f"Upload successful for las")  

'''
ECIST SITREP
'''
if True:

    ### Import settings from the .env file
    env = import_settings(config, "ecist")

    # Set parameters
    DATE_RANGE = config["ecist_sitrep"]["date"]["date_range"]
    INDICATOR_LIST = config["ecist_sitrep"]["base"]['indicator_list'] # import metric list
    ORG_LIST = config["ecist_sitrep"]["base"]['org_list'] # import metric list
    #Should the data file be archived on completion? (Set to False when debugging)
    ARCHIVE_FILE = False

    sitrep_file_path = getenv("NETWORKED_DATA_PATH_ECIST")

    # Get the latest file
    latest_file = [file for file in os.listdir(sitrep_file_path) if file.endswith(".xlsb")] #This checks every file in the current directory using the listdir function and then returns only files that end in ".xlsb" which are excel binary files

    #Error checking to make sure the new data file is in the directory as expected.
    #If the new data file can't be unidentified then raise an error so the person running this code can fix it
    if len(latest_file) != 1:
        #If there are no xlsb files found
        if len(latest_file) == 0:
            raise Exception(f"Error, no excel binary file (xlsb) found in the directory.")
        #If there are multiple xlsb files found
        if len(latest_file) > 1:
            raise Exception(f"Error, multiple excel binary file (xlsb) found in the directory or the UEC Dashboard file is open.")


    #Get the file name from the latest_file variable
    filename = os.path.join(sitrep_file_path,latest_file[0])
    #The name of the sheet with the data on it
    sheet_name = "New Raw Data"

    new_sitrep_data = pd.read_excel(filename, sheet_name=sheet_name)
    new_sitrep_data.rename(columns=clean_column_name, inplace=True)
    new_sitrep_data.columns = map(str.lower, new_sitrep_data.columns)

    # Filter data to relevant sites, indicators and time period
    ## Date
    new_sitrep_data["period"] = pd.to_datetime(new_sitrep_data["period"], origin="1899-12-30", unit="D")
    new_sitrep_data['cutoff'] = pd.Timestamp(datetime.datetime.now()).date()-pd.to_timedelta(DATE_RANGE, unit='d')
    new_sitrep_data = new_sitrep_data.query("period >= cutoff")
    ## Site
    new_sitrep_data = new_sitrep_data.query('orgcode in @ORG_LIST')
    new_sitrep_data["orgcode"] = new_sitrep_data.apply(lambda x: x["site_code"] if x["orgcode"] == "RAL" else x["orgcode"], axis=1)
    new_sitrep_data = new_sitrep_data.drop(['trust_name', 'site_name', 'site_code', 'stp_name','region_name'], axis=1) # site specific columns not needed 
    ## Indicator
    new_sitrep_data = new_sitrep_data.melt(id_vars = ['orgcode', 'period'], var_name='indicatorKeyName', value_name='value') # lengthen data to allow filter
    new_sitrep_data = new_sitrep_data.query('indicatorKeyName in @INDICATOR_LIST') # filter metrics list
    new_sitrep_data = new_sitrep_data[['period', 'orgcode','indicatorKeyName','value']].reset_index(drop=True) 

    # Inter porcessing
    new_sitrep_data.rename(columns={'period': 'reportDate', 'orgcode': 'provider_code'},  inplace=True)
    new_sitrep_data = new_sitrep_data[['indicatorKeyName', 'provider_code', 'reportDate', 'value']]
    new_sitrep_data = new_sitrep_data.groupby(['reportDate', 'provider_code', 'indicatorKeyName'])['value'].sum().reset_index()
    new_sitrep_data = new_sitrep_data.drop_duplicates()
    new_sitrep_data['source'] = 'ecist_sitrep'
    new_sitrep_data['metric_type'] = 'actual'

    new_sitrep_data = new_sitrep_data[['source', 'indicatorKeyName', 'provider_code', 'reportDate', 'metric_type', 'value']]
    new_sitrep_data.to_csv('inter.csv', mode='a', index=False, header=False)

    ## Sandpit upload
    # reshape for sandpit
    new_sitrep_data = new_sitrep_data.drop(['source', 'metric_type'], axis=1).reset_index(drop=True)
    new_sitrep_data = new_sitrep_data.pivot(index=['reportDate', 'provider_code'], columns='indicatorKeyName', values='value').reset_index()
    new_sitrep_data = new_sitrep_data.rename_axis(None, axis = 1)

    date_end = new_sitrep_data.max().iloc[0].date()
    date_start = (date_end - timedelta(days=14))

    # upload to sandpit - once suficiently generalised
    query_del = get_delete_query(date_start, date_end, ["RAL01" "RAL26", "RALC7", "RAP", "RKE", "RRV"], env)
    upload_request_data(new_sitrep_data, query_del, env)
    print(f"Upload successful for ecist")  