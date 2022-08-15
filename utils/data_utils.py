import requests
import logging
import json
import pandas as pd
import utils
import date_utils
from datetime import datetime
from itertools import chain

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)


def get_hawker_mrt_dist_df(config):
    hawker_mrt_dist_json = utils.get_json_data_from_url(config.get("urls", "mrt_hawker_distances"))
    df = pd.DataFrame.from_records(hawker_mrt_dist_json)
    return df


def get_hawker_data_df(config):
    hawker_data_json = utils.get_json_data_from_url(config.get("urls", "hawker_data"))
    df = pd.DataFrame.from_records(hawker_data_json)
    df = prepare_hawker_data(df)
    return df


def get_hawkers_washing(df, this_week=False, next_week=False) -> pd.DataFrame:
    start_date = end_date = date_utils.get_date_today()
    if this_week:
        start_date, end_date = date_utils.get_date_range_in_weeks(start_week=0, end_week=1)
    elif next_week:
        start_date, end_date = date_utils.get_date_range_in_weeks(start_week=1, end_week=2)

    query_date_range = set(pd.date_range(start_date, end_date, freq='D').tolist())

    cleaning_hawkers = []
    for quarter in range(1, 5):
        quarter_date_range_col = f"q{quarter}_date_range"
        quarter_start_col = f"q{quarter}_start"
        quarter_end_col = f"q{quarter}_end"
        cleaning_hawkers = df[
            (df[quarter_date_range_col].apply(lambda qdr: len(set.intersection(set(qdr.tolist()),
                                                                               query_date_range)) > 0))
        ][[quarter_start_col, quarter_end_col]].rename(columns={
            quarter_start_col: "start_date",
            quarter_end_col: "end_date"
        })

        if len(cleaning_hawkers) > 0:
            break

    # Format for printing
    cleaning_hawkers['start_date'] = cleaning_hawkers['start_date'].apply(lambda dt: dt.strftime("%d/%m"))
    cleaning_hawkers['end_date'] = cleaning_hawkers['end_date'].apply(lambda dt: dt.strftime("%d/%m"))

    return cleaning_hawkers


def hawkers_not_existing(df) -> List:
    return list(df[df['not_existing']].index)


def prepare_hawker_data(df: pd.DataFrame) -> pd.DataFrame:
    df['not_existing'] = df['hawker_status'].apply(lambda s: "existing" not in s.lower())
    df = df.set_index("hawker_name")

    cleaning_columns = ["q1_start", "q1_end", "q2_start", "q2_end", "q3_start", "q3_end", "q4_end"]
    for c in cleaning_columns:
        df[c] = df[c].apply(lambda d: datetime.strptime(d, "%d/%m/%Y"))

    for q in range(1, 5):
        quarter_start = f"q{q}_start"
        quarter_end = f"q{q}_end"
        df[f"q{q}_date_range"] = df[[quarter_start, quarter_end]] \
            .apply(lambda row: pd.date_range(row[quarter_start], row[quarter_end], freq='D'), axis=1)

    return df


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

logging.info("Dumping results into JSON file")
with open("../data/hawker_data.json", 'w') as f:
    json.dump(hawker_data, f)
