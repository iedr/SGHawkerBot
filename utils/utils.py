from typing import List, Dict
from utils.emojis import EMOJIS
import requests
import random
import sys
import logging


def get_json_data_from_url(url: str) -> List[Dict]:
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        sys.exit(-1)


def get_shuffled_emojis() -> List:
    emojis = list(EMOJIS)
    random.shuffle(emojis)
    return emojis


def log_command_pressed(update):
    user = update.message.from_user
    username = user.username
    user_id = user.id
    first_name = user.first_name

    if update.message.location is not None:
        lat = update.message.location.latitude
        long = update.message.location.longitude
        logging.info(f"{username}/{first_name} ({user_id}) sent a location: {lat}, {long}.")
    else:
        command = update.message.text[:255]
        logging.info(f"{username}/{first_name} ({user_id}) sent {command}.")
