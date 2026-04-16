

from  graphdatascience import GraphDataScience
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
import os


def get_gds_connection():

    load_dotenv()

    return GraphDataScience(str(os.getenv("db_uri")), auth=(str(os.getenv("db_user")), str(os.getenv("db_password"))))