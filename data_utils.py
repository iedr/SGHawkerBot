import requests
import logging
import json
import pandas as pd
from itertools import chain

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def fetch_records_from_api() -> list:
    all_records = []
    num_results_per_page = 100
    should_get_more_results = True
    base_url = 'https://data.gov.sg'
    api_link = '/api/action/datastore_search?resource_id=b80cb643-a732-480d-86b5-e03957bc82aa'

    while should_get_more_results:
        logging.info("Fetching records from %s..." % api_link)
        resp = requests.get(base_url + api_link)
        resp_json = resp.json()

        if 'result' not in resp_json:
            logging.error("'result' key not found in JSON response")
            break

        result = resp_json['result']

        if 'records' not in result:
            logging.error("'records' key not found in JSON response")
            break

        records = result['records']
        all_records.append(records)
        should_get_more_results = len(records) >= num_results_per_page

        if '_links' not in result and 'next' not in result['_links']:
            logging.error("'_links' key not found in JSON response")
            break

        api_link = result['_links']['next']

    all_records = list(chain.from_iterable(all_records))
    logging.info("Number of records retrieved: %s" % len(all_records))
    return all_records


def get_gmaps_url(hawker_name):
    gmaps_base_url = "https://www.google.com/maps/search/?api=1&query="
    return gmaps_base_url + "+".join(hawker_name.split())


def get_relevant_fields_as_dict(record) -> dict:
    hawker_name = record['name']
    gmaps_url = get_gmaps_url(hawker_name)

    lat = float(record['latitude_hc'])
    long = float(record['longitude_hc'])

    return {
        'address': record['address_myenv'],
        'description': record['description_myenv'],
        'hawker_name': hawker_name,
        'hawker_photo_url': record['photourl'],
        'hawker_gmaps_url': gmaps_url,
        'hawker_status': record['status'],
        'longitude': long,
        'latitude': lat,
        'hawker_coords': (lat, long),
        'q1_start': record['q1_cleaningstartdate'].replace("TBC", "14/1/1990"),
        'q1_end': record['q1_cleaningenddate'].replace("TBC", "14/1/1990"),
        'q2_start': record['q2_cleaningstartdate'].replace("TBC", "14/1/1990"),
        'q2_end': record['q2_cleaningenddate'].replace("TBC", "14/1/1990"),
        'q3_start': record['q3_cleaningstartdate'].replace("TBC", "14/1/1990"),
        'q3_end': record['q3_cleaningenddate'].replace("TBC", "14/1/1990"),
        'q4_start': record['q4_cleaningstartdate'].replace("TBC", "14/1/1990"),
        'q4_end': record['q4_cleaningenddate'].replace("TBC", "14/1/1990"),
    }


logging.info("Fetching hawker records from API")
api_records = fetch_records_from_api()
hawker_data = [get_relevant_fields_as_dict(r) for r in api_records]
hawker_data_df = pd.DataFrame(hawker_data)

logging.info("Dumping results into CSV file")
hawker_data_df.to_csv("hawker_data.csv")
