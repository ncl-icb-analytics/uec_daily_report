#Import modules
import pandas as pd
import ncl_sqlsnippets as snips
import toml
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import types
from os import getenv
import re

### Load environment variables 
config = toml.load("./config.toml")
load_dotenv(override=True)

def clean_column_name(column_name):
    column_name = column_name.strip()
    #column_name = re.sub(r"[^\w\s]", "", column_name)
    column_name = re.sub(r"\n", "", column_name)
    column_name = re.sub(r"\s+", "_", column_name)
    return column_name



### Generate file for intermediate wrangle:

pd.DataFrame([], 
             columns=['source', 'indicatorKeyName', 'site_code_ref', 
                      'ReportDate', 'metric_type', 'value']
             ).to_csv('inter.csv', mode='w', index=False, header=True)


# Set parameters
DATE_RANGE = config["ecist_sitrep"]["date"]["date_range"]
IndicatorList = config["ecist_sitrep"]["base"]['indicator_list'] # import metric list
SiteList = config["ecist_sitrep"]["base"]['site_list'] # import metric list
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
new_sitrep_data['cutoff'] = pd.Timestamp(datetime.now()).date()-pd.to_timedelta(DATE_RANGE, unit='d')
new_sitrep_data = new_sitrep_data.query("period >= cutoff")
## Site
new_sitrep_data = new_sitrep_data.query('orgcode in @SiteList')
new_sitrep_data = new_sitrep_data.drop(['trust_name', 'site_name', 'site_code', 'stp_name','region_name'], axis=1) # site specific columns not needed 
## Indicator
new_sitrep_data = new_sitrep_data.melt(id_vars = ['orgcode', 'period'], var_name='indicatorKeyName', value_name='value') # lengthen data to allow filter
new_sitrep_data = new_sitrep_data.query('indicatorKeyName in @IndicatorList') # filter metrics list
new_sitrep_data = new_sitrep_data[['period', 'orgcode','indicatorKeyName','value']].reset_index(drop=True) 

# Inter porcessing
new_sitrep_data.rename(columns={'period': 'reportDate', 'orgcode': 'site_code_ref'},  inplace=True)
new_sitrep_data = new_sitrep_data[['indicatorKeyName', 'site_code_ref', 'reportDate', 'value']]
new_sitrep_data = new_sitrep_data.groupby(['reportDate', 'site_code_ref', 'indicatorKeyName'])['value'].sum().reset_index()
new_sitrep_data = new_sitrep_data.drop_duplicates()
new_sitrep_data['source'] = 'ecist_sitrep'
new_sitrep_data['metric_type'] = 'actual'

new_sitrep_data = new_sitrep_data[['source', 'indicatorKeyName', 'site_code_ref', 'reportDate', 'metric_type', 'value']]
new_sitrep_data.to_csv('inter.csv', mode='a', index=False, header=False)

## Sandpit upload
# reshape for sandpit
new_sitrep_data = new_sitrep_data.drop(['source', 'metric_type'], axis=1).reset_index(drop=True)
new_sitrep_data = new_sitrep_data.pivot(index=['reportDate', 'site_code_ref'], columns='indicatorKeyName', values='value').reset_index()
new_sitrep_data = new_sitrep_data.rename_axis(None, axis = 1)

