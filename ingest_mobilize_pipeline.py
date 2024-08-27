import requests
from google.cloud import bigquery
import os
import json
import pandas as pds

def download_json_data(base_url:str,endpoint:str,headers:str) -> json[list[dict]]:
    """download_data retrieves json from API endpoint
    
    Args:
    base_url - base url for API (should end in /)
    endpoint- endpoint locations to be appended to base url
    headers- Authorization header. Should be stored in Environment varaible

    Returns:
    JSON results as List of Dict
    """
    response = requests.get(base_url + endpoint, headers=headers)
    result = response.json
    return result


def save_json_data(data: list[dict],filepath:str) -> str:
    """Saves JSON data to specified file location
    
    Args:
    data- JSON data (List Of Dicts)
    filepath- target file path where JSON will be saved
    """
    f = open(filepath, "w")
    json.dump(data, f, indent=4)


def load_events(filepath: str):
    """Loads Big Query Mobilize Events table from JSON in specified location

    Args:
    filepath- location of JSON file to load

    Raises:
    Exception row fails to write to BigQuery
    """
    file = open(filepath, "r")
    data = pds.read_json(filepath)

    try:
        client = bigquery.Client()
        table = client.get_table("wfp-data-project.mobilize.events")
        event = {
            key: value
            for key, value in row["event"].items()
            if key
            in (
                "created_date",
                "modified_date",
                "id",
                "title",
                "event_type",
                "summary",
                "description",
            )
        }
        client.load_table_from_dataframe(table, [event])
    except:
        print("error loading row")

# Run Data
filepath="data/attendances.json"
data = download_json_data(base_url="https://api.mobilize.us/v1/",endpoint="attendances",headers={"Authorization": "Bearer {}".format(os.environ.get("MOBILIZE_API_KEY"))})
filepath = save_json_data(data,filepath)
load_events(filepath)
