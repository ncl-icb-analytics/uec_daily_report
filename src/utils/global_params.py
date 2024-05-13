import toml
from os import getenv

config = toml.load("./config.toml")
load_dotenv(override=True)


params_smart ={
        "API_URL": config["smart_api"]["base"]["api_url"],
        "API_KEY": getenv("SMART_API_KEY"),
        "SITES": config["smart_api"]["base"]["sites"],
        
        "DATE_END": config["smart_api"]["date"]["end"],
        "DATE_WINDOW": config["smart_api"]["date"]["window"],
        
        "WAIT_PERIOD": config["smart_api"]["wait"]["period"],
        "WAIT_COOLOFF": config["smart_api"]["wait"]["cooloff"],

        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": config["database"]["sql_database"],
        "SQL_SCHEMA": config["database"]["sql_schema"],
        "SQL_TABLE": config["smart_api"]["base"]["sql_table"],
    }

params_las ={
        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": config["database"]["sql_database"],
        "SQL_SCHEMA": config["database"]["sql_schema"],
        "SQL_TABLE": config["las"]["base"]["sql_table"],
    }

params_ecist ={
        "SQL_ADDRESS": getenv("SQL_ADDRESS"),
        "SQL_DATABASE": config["database"]["sql_database"],
        "SQL_SCHEMA": config["database"]["sql_schema"],
        "SQL_TABLE": config["ecist_sitrep"]["base"]["sql_table"],
    }