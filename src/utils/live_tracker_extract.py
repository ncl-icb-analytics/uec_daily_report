import pandas as pd
import ncl_sqlsnippets as snips
import re
from datetime import date, datetime as dtt
import os

#pip install python-dotenv
from dotenv import load_dotenv
from os import getenv, rename

#Outputs the result status of each dataset
def print_status(status, message, env=None):

    if status == 200:
        print(f"New data processed for {message}")

    elif status == 400:
        print(message)

    elif status == 401:
        print(f"An error occured when trying to upload the new data in", 
              f"{message}. Please check the new data does not overlap", 
              "existing data in the table.\n")

    elif status == 402:
        print(status, " ", message)

    elif status == 403:
        print("An error occured when processing the Virtual Ward file.", 
              f"Please check the '{env["VW_SHEET_NAME"]}' sheet in the file",
              "for errors.")

    elif status == 404:
        print("No new data files found.")

    else:
        print(status, message)

#Scans the new data folder for new data
def scan_new_files(datasets, env):
    dir = getenv("NETWORKED_DATA_PATH_LIVE_TRACKER")

    #List of files in the new_data directory
    new_data_files = os.listdir(dir)

    #Categorise files by dataset
    scanned_files = {}

    for ds in datasets:
        ds_name = env[ds]

        scanned_files[ds] = []
        
        for ndf in new_data_files:
            if os.path.isfile(os.path.join(dir, ndf)) and ndf.endswith(".xlsx"):
                if ds_name in ndf or ds in ndf:
                    if not(ndf.startswith("~$")):
                        scanned_files[ds].append(ndf)

    return scanned_files

#Process settings
def import_settings_ef():
    load_dotenv(override=True)

    return {
        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": getenv("SQL_DATABASE"),
        "SQL_SCHEMA": getenv("SQL_SCHEMA"),

        "MO_SHEET_NAME": getenv("MO_SHEET_NAME"),
        "MO_TABLE_HEADING": getenv("MO_TABLE_HEADING"),
        "MO_SQL_TABLE": getenv("MO_SQL_TABLE"),
        "MO_DATE_OVERWRITE": getenv("MO_DATE_OVERWRITE"),

        "P2_SHEET_NAME": getenv("P2_SHEET_NAME"),
        "P2_SQL_TABLE": getenv("P2_SQL_TABLE"),
        "P2_DATE_OVERWRITE": getenv("P2_DATE_OVERWRITE"),

        "VW_SHEET_NAME": getenv("VW_SHEET_NAME"),
        "VW_SQL_TABLE": getenv("VW_SQL_TABLE")
    }

#Archive the data file
def archive_data_file_lt(data_file, dir, ds, date_label):
    #Rename the current file to archive it
    new_filename = f"{dir}/archive/{ds} {date_label}.xlsx"
    rename(data_file, new_filename)

#Get date_data from a dirty date column
def get_date_data(df, date_extract, ds, ndf):

    date_str_arr = df["date_update"].values

    for i, date_str in enumerate(date_str_arr):
        #If recognised as a date object
        if isinstance(date_str, dtt):
            date_str_arr[i] = date_str.date()
        elif isinstance(date_str, date):
            pass
        #Not recognised as a date object
        elif type(date_str) == type(""):
            nums = re.split(r'\D+', date_str)

            if len(nums[0]) == 4:
                date_str_arr[i] = date(int(nums[0]), int(nums[1]), int(nums[2]))
            else:
                #Make sure year is formatted properly
                year = int(nums[2])
                if year < 100:
                    year += 2000

                #Save as a date type value
                date_str_arr[i] = date(year, int(nums[1]), int(nums[0]))
        else:
            print(f"Warning, the {ds} file '{ndf}' is missing date(s) in the Last Updated column.")
            date_str_arr[i] = date(1970, 1, 1)

    #Check for any dates in the future
    for date_value in date_str_arr:
        if str(date_value) >= date_extract:
            print((f"Warning: The {ds} file '{ndf}' contained a date from either" 
                  " today or the future and won't be processed.\nPlease confirm the"
                  " file is for the correct date and correct any typos if the wrong" 
                  " date is used in the file."))
            return False

    return date_str_arr.max()

def create_tracker_table(engine, ds):
    create_table_file = f"./src/sql_table_create/create_table_trackers_{ds}.sql"

    f = open(create_table_file, 'r')
    sql_query = f.read()
    f.close()

    snips.execute_query(engine, sql_query)

#ef function for the Pathway MOs
def ef_mo(env, ndf):
    
    archive = env["ARCHIVE_FILE"]
    date_extract = env["date_extract"]
    data_dir = getenv("NETWORKED_DATA_PATH_LIVE_TRACKER")
    ds = "mo"

    #Process date_extract
    if env["MO_DATE_OVERWRITE"] != "":
        try:
            dtt.strptime(env["MO_DATE_OVERWRITE"], "%Y-%m-%d")
        except ValueError:
            return 400, f"The Daily Delay MO_DATE_OVERWRITE value ({env['MO_DATE_OVERWRITE']}) is not a valid YYYY-MM-DD value." 
    
    #Load the file
    df_src = pd.read_excel(data_dir + ndf, 
                           sheet_name= env["MO_SHEET_NAME"])


    #Trim the src dataframe down to the table of relevant data
    table_start_index = df_src.index[df_src.iloc[:, 0] == env["MO_TABLE_HEADING"]][0]
    table_end_index = [x for x in df_src.index[df_src.iloc[:, 0] == "Total"] if x > table_start_index][0]

    df_provider_table = df_src.iloc[table_start_index + 1:table_end_index, :6]
    df_provider_table.columns = ["provider", "p1", "p2", "p3", "total", "date_update"]

    df_provider_table = df_provider_table.reset_index(drop=True)

    date_data = get_date_data(df_provider_table, date_extract, ds, ndf)

    #Check if the file contains valid dates
    if not date_data:
        return 400, ""

    #Format the dataframe for uploading
    df_output = pd.melt(df_provider_table.iloc[:, :4], id_vars=["provider"], var_name="metric_name", value_name="metric_value")
    df_output["date_extract"] = date_extract
    df_output["date_data"] = date_data

    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])

    #Use org lookup file to get provider codes
    site_id_map = pd.read_csv("./lookups/org_lookup.csv")
    mo_id_map = site_id_map[site_id_map["dataset"] == "mo"]
    df_output = df_output.merge(mo_id_map, how="left", 
                                       left_on="provider", 
                                       right_on="dataset_reference")
    df_output = df_output.drop(["dataset", "dataset_reference", "provider"], axis=1)

    if not snips.table_exists(engine, env['MO_SQL_TABLE'], env['SQL_SCHEMA']):
        try:
            create_tracker_table(engine, ds)
        except Exception as e:
            print(e)
            return 402, f"Unable to create table for {ds}. Try running the create table sql in docs/sql/live_tracker for this dataset."

    try:
        snips.upload_to_sql(df_output, engine, env["MO_SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=300)
    except:
        return 401, ndf

    if archive:
        try:
            archive_data_file_lt(data_dir + ndf, data_dir, ds, date_data)
        except:
            return 402, f"Data uploaded but archive for file {ndf} failed."

    return 200, f"{ds} {date_data}.xlsx."

#ef function for the P2 Occupancy
def ef_p2(env, ndf):

    archive = env["ARCHIVE_FILE"]
    date_extract = env["date_extract"]
    data_dir = getenv("NETWORKED_DATA_PATH_LIVE_TRACKER")
    ds = "p2"


    #Process date_extract
    if env["P2_DATE_OVERWRITE"] != "":
        try:
            dtt.strptime(env["P2_DATE_OVERWRITE"], "%Y-%m-%d")
        except ValueError:
            #return 400, f"The Daily Delay P2_DATE_OVERWRITE value ({env['P2_DATE_OVERWRITE']}) is not a valid YYYY-MM-DD value."
            print(f"The Daily Delay P2_DATE_OVERWRITE value ({env['P2_DATE_OVERWRITE']}) is not a valid YYYY-MM-DD value.")

    #Load the file
    try:
        df_src = pd.read_excel(getenv("NETWORKED_DATA_PATH_LIVE_TRACKER") + ndf, sheet_name= env["P2_SHEET_NAME"])
    except PermissionError as e:
        return 400, e

    df_trimmed = df_src.copy().iloc[1:, 1:]
    df_trimmed.columns = df_src.iloc[0, 1:]

    #Attempt to process files but raise expection if column name changes
    try:

        #Remove total and subtotal rows
        df_trimmed = df_trimmed[df_trimmed["Provider"].notna()]
        df_trimmed.loc[:, "Units"] = df_trimmed["Units"].str.replace('\n', " ")

        #Add columns for output metrics
        df_trimmed["beds_available"] = \
            df_trimmed["Max Capacity"] + \
            df_trimmed["Beds closed to new admissions"]

        df_trimmed["beds_occupied"] = \
            df_trimmed["P2 Multipathology Rehab Beds Currently Occupied"] + \
            df_trimmed["P2 Stroke/Neuro Beds Currently Occupied"] + \
            df_trimmed["P2 Non-Rehab Beds Currently Occupied"] + \
            df_trimmed["Planned admissions in next 24hrs"] - \
            df_trimmed["Expected Discharges for today"]

    #Error handling for duplicate columns
    except ValueError as ve:
        #Check if issue is duplicate columns
        #if ve.args[0][0:14] == "cannot reindex":
            #Duplicate
            #pass

        return 400, (f"Duplicate columns found in '{ndf}', " + 
                     "please remove duplicate columns from the source file.") 
    
    #Error handling for missing columns
    except Exception as e:
        return 400, f"Column {e} no longer found in '{ndf}'. Please check if there is data missing from the source file."

    df_trimmed = df_trimmed[["Provider", "Units", "beds_available", "beds_occupied", "Last Updated (please give time and date)"]]
    df_trimmed.columns = ["provider", "unit", "beds_available", "beds_occupied", "date_update"]

    df_output = df_trimmed.copy().iloc[:, :4]

    df_output["date_extract"] = date_extract
    date_data = get_date_data(df_trimmed, date_extract, ds, ndf)

    #Check if the date in the file is valid and halt processing if not
    if not date_data:
        return 400, ""

    df_output["date_data"] = date_data

    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])

    if not snips.table_exists(engine, env['P2_SQL_TABLE'], env['SQL_SCHEMA']):
        try:
            create_tracker_table(engine, ds)
        except Exception as e:
            print(e)
            return 402, f"Unable to create table for {ds}. Try running the create table sql in docs/sql/live_tracker for this dataset."

    try:
        snips.upload_to_sql(df_output, engine, env["P2_SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=300)
    except:
        return 401, ndf

    if archive:
        try:
            archive_data_file_lt(data_dir + ndf, data_dir, ds, date_data)
        except:
            return 402, f"Data uploaded but archive for file {ndf} failed."

    return 200, f"{ds} {date_data}.xlsx."

#ef function for the Virtual Wards
def ef_vw(env, ndf):
    archive = env["ARCHIVE_FILE"]
    date_extract = env["date_extract"]
    data_dir = getenv("NETWORKED_DATA_PATH_LIVE_TRACKER")
    ds = "vw"

    #Load the file
    df_output = pd.read_excel(getenv("NETWORKED_DATA_PATH_LIVE_TRACKER") + ndf, sheet_name= env["VW_SHEET_NAME"])

    df_output.columns = ["date_data", "capacity", "occupied", "system_value", "includes_paediatric"]

    engine = snips.connect(env["SQL_ADDRESS"], env["SQL_DATABASE"])

    try:
        if snips.table_exists(engine, env['VW_SQL_TABLE'], env['SQL_SCHEMA']):
            snips.execute_query(engine, f"TRUNCATE TABLE {env['SQL_DATABASE']}.{env['SQL_SCHEMA']}.{env['VW_SQL_TABLE']};")
        else:
            try:
                create_tracker_table(engine, ds)
            except Exception as e:
                print(e)
                return 402, f"Unable to create table for {ds}. Try running the create table sql in docs/sql/live_tracker for this dataset."
            
        snips.upload_to_sql(df_output, engine, env["VW_SQL_TABLE"], env["SQL_SCHEMA"], replace=False, chunks=300)
    except:
        return 403, ndf

    if archive:
        try:
            archive_data_file_lt(data_dir + ndf, data_dir, ds, date_extract)
        except:
            return 402, f"Data uploaded but archive for file {ndf} failed."

    return 200, f"{ds} {date_extract}.xlsx."

#Control function to decide which ef function to use
def ef_controller (dataset, params, new_data_files):

    if dataset == 'vw' and len(new_data_files) > 1:
        print_status(501, f"Virtual Ward extraction only supports single file additions. Please ensure there is only 1 Virtual Ward tracker file in the new_data directory.")
    else:
        for ndf in new_data_files:
            try:
                #Run the relevant extract function
                if dataset == "mo":
                    status, message = ef_mo(params, ndf)
                elif dataset == "p2":
                    status, message = ef_p2(params, ndf)
                elif dataset == "vw":
                    status, message = ef_vw(params, ndf)
                else:
                    print_status(500, f"Dataset {dataset} is not supported.")

                #If an issue occurs then report the issue
                print_status(status, message, params)

            except Exception as e:
                print(400, e)