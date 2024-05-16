import datetime
from os import getenv
from dotenv import load_dotenv

def import_settings(config, ds):

    load_dotenv(override=True)

    if ds == "smart_api":
        params_smart ={
                "API_URL": config["smart_api"]["base"]["api_url"],
                "API_KEY": getenv("SMART_API_KEY"),
                "SITES": config["smart_api"]["base"]["sites"],
                
                "DATE_END": config["smart_api"]["date"]["end"],
                "DATE_WINDOW": config["smart_api"]["date"]["window"],
                
                "WAIT_PERIOD": config["smart_api"]["wait"]["period"],
                "WAIT_COOLOFF": config["database"]["sql_cooloff"],

                "SQL_ADDRESS": getenv("SQL_ADDRESS"),
                "SQL_DATABASE": config["database"]["sql_database"],
                "SQL_SCHEMA": config["database"]["sql_schema"],
                "SQL_TABLE": config["smart_api"]["base"]["sql_table"],
            }
        return params_smart
    
    elif ds == "las":
        params_las ={
                "SQL_ADDRESS": getenv("SQL_ADDRESS"),
                "SQL_DATABASE": config["database"]["sql_database"],
                "SQL_SCHEMA": config["database"]["sql_schema"],
                "SQL_TABLE": config["las"]["base"]["sql_table"],
                "WAIT_COOLOFF": config["database"]["sql_cooloff"],
            }
        return params_las
    
    elif ds == "ecist":
        params_ecist ={
                "SQL_ADDRESS": getenv("SQL_ADDRESS"),
                "SQL_DATABASE": config["database"]["sql_database"],
                "SQL_SCHEMA": config["database"]["sql_schema"],
                "SQL_TABLE": config["ecist_sitrep"]["base"]["sql_table"],
                "WAIT_COOLOFF": config["database"]["sql_cooloff"],
            }
        return params_ecist
    
    elif ds == "live_tracker":
        params_tracker ={
                "ARCHIVE_FILE": config["live_tracker"]["archive_file"],

                "mo" : config["live_tracker"]["mo"]["mo"],
                "p2" : config["live_tracker"]["p2"]["p2"],
                "vw" : config["live_tracker"]["vw"]["vw"],

                "SQL_ADDRESS": getenv("SQL_ADDRESS"),
                "SQL_DATABASE": config["database"]["sql_database"],
                "SQL_SCHEMA": config["database"]["sql_schema"],

                "MO_SHEET_NAME": config["live_tracker"]["mo"]["mo_sheet_name"],
                "MO_TABLE_HEADING": config["live_tracker"]["mo"]["mo_table_heading"],
                "MO_SQL_TABLE": config["live_tracker"]["mo"]["mo_sql_table"],
                "MO_DATE_OVERWRITE": config["live_tracker"]["mo"]["mo_date_overwrite"],

                "P2_SHEET_NAME": config["live_tracker"]["p2"]["p2_sheet_name"],
                "P2_SQL_TABLE": config["live_tracker"]["p2"]["p2_sql_table"],
                "P2_DATE_OVERWRITE": config["live_tracker"]["p2"]["p2_date_overwrite"],

                "VW_SHEET_NAME": config["live_tracker"]["vw"]["vw_sheet_name"],
                "VW_SQL_TABLE": config["live_tracker"]["vw"]["vw_sql_table"]
            }
        params_tracker["date_extract"] = datetime.date.today().strftime("%Y-%m-%d")

        return params_tracker
    
    else:
        raise ValueError (f"{ds} is not supported.")