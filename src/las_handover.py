import sys
import pathlib
import os 
import re
from os import getenv
from datetime import datetime

from dotenv import load_dotenv

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
LAS
'''
config = toml.load("./config.toml")

# Load the sheet into a dataframe (google pandas dataframes)
#df1 original dataset
las_file_path = getenv("NETWORKED_DATA_PATH_LAS")
#las_file_path = os.path.join(N_drive, "/data", "/las_handover", "Ambulance_Handover_Report_v2.1.xlsb") 
las_data = pd.read_excel(las_file_path, sheet_name= "Data_Ambulance_Handovers")

def clean_column_name(column_name):
    column_name = column_name.strip()
    #column_name = re.sub(r"[^\w\s]", "", column_name)
    column_name = re.sub(r"\n", "", column_name)
    column_name = re.sub(r"\s+", "_", column_name)
    return column_name


las_data.rename(columns=clean_column_name, inplace=True)
las_data.columns = map(str.lower, las_data.columns)

las_data = las_data.query('stp_code == "QMJ"').reset_index(drop = True)
las_data = las_data.drop(['stp_code', 'stp_short', 'weekday', 'id'], axis=1)
las_data = las_data.melt(id_vars = ['hospital_site', 'period'], var_name='indicatorKeyName', value_name='value')
IndicatorList = config["las"]["base"]['indicator_list']
las_data = las_data.query('indicatorKeyName in @IndicatorList')
las_data = las_data[['period', 'hospital_site','indicatorKeyName','value']].reset_index(drop=True)
las_data['period'] = pd.to_datetime(las_data['period'], unit='D', origin='1899-12-30')#, errors='coerce')
las_data['cutoff'] = pd.Timestamp(datetime.now()).date()-pd.to_timedelta(14, unit='d')
las_data = las_data.query("period >= cutoff")
print(las_data)
#print(las_data.columns.tolist())


# Hospital Site (filtered), Total Handover, Over 45 minutes, Over 120 minutes
# STP code =  QMJ (NCL) 