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
                "ARCHIVE_FILE": getenv("ARCHIVE_FILE"),

                "mo" : getenv("MO"),
                "p2" : getenv("P2"),
                "vw" : getenv("VW"),

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
        params_tracker["date_extract"] = datetime.date.today().strftime("%Y-%m-%d")

        return params_tracker
    
    else:
        raise ValueError (f"{ds} is not supported.")