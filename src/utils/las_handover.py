import json
import pandas as pd
import numpy as np
import os
import requests
import time
from os import getenv
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

#Library from https://github.com/ncl-icb-analytics/sqlsnippets
import ncl_sqlsnippets as snips

from datetime import date
import re
import shutil