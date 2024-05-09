import sys
import pathlib
import os 

from dotenv import load_dotenv

from utils.smart_api import *

config = toml.load("./config.toml")

load_dotenv(override=True)

# Generate file for intermediate wrangle:
with open('inter.csv', 'w') as file:
    writer = csv.writer(file)
    writer.writerow(['source', 'indicatorKeyName', 'siteName', 'ReportDate','date_start', 'date_end', 'metric_type', 'value'])

'''
Pull from smart API
'''

print("API program starting...")

#Import settings from the .env file
env = import_settings(config)

#Process the settings to get the start and end dates
date_end = process_date_end(env["DATE_END"])
date_start = process_date_window(env["DATE_WINDOW"], date_end)

#Determine how many runs are needed to get all the data
runs = calculate_runs(date_start, date_end)

#Execute the runs on the API and upload the result to the sandpit
# This needs to be split out
execute_runs(config, runs, env)

print("All API pulls complete")