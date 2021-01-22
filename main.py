import telegram
from telegram.ext import Updater
from fuzzywuzzy import fuzz
import logging
import pandas as pd
import requests
import os
import sys
import random
import geopy.distance
from datetime import datetime
from telegram.ext import CommandHandler, MessageHandler, ConversationHandler, Filters, PicklePersistence


def log_command_pressed(update):
    user = update.message.from_user
    username = user.username
    user_id = user.id
    first_name = user.first_name
    command = update.message.text[:255]

    if update.message.location is not None:
        lat = update.message.location.latitude
        long = update.message.location.longitude
        logging.info(f"{username}/{first_name} ({user_id}) sent a location: {lat}, {long}.")
    else:
        logging.info(f"{username}/{first_name} ({user_id}) sent {command}.")


def get_shuffled_emojis():
    emojis = EMOJIS[:]
    random.shuffle(emojis)
    return emojis


def start(update, context):
    log_command_pressed(update)

    reply_text = [
        f"Hello, {update.message.from_user.first_name}! This is SG Hawker bot.",
        f"\nDo you need information about SG's hawker centres?",
        f"\n\t 🧹 /closed tells you which hawker centres are closed today.",
        f"\t 🔍 /search Input a search term and I'll tell you the hawker centres that best match your query."
        f"For example, <b>/search bedok</b> or <b>/search west coast drive</b>.",
        f"\t 🚝 /mrt lets you know the hawker centres near an MRT/LRT station.",
        f"\t 🧭 Want to know which hawker centres are near you? Simply send me your location!",
        f"\t ℹ Need more information about me? Type /info.",
    ]

    reply_text = "\n".join(reply_text)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=reply_text,
                             parse_mode=telegram.ParseMode.HTML,
                             disable_web_page_preview=True,)


def get_info(update, context):
    log_command_pressed(update)

    reply_text = [
        f"Hello, {update.message.from_user.first_name}! This is SG Hawker bot.",
        f"\nSome things about me:",
        f"\t• I am written using the Python library <a "
        f"href=\"https://github.com/python-telegram-bot/python-telegram-bot\">python-telegram-bot</a>.",
        f"\t• My data comes from <a href=\"https://data.gov.sg/\">data.gov.sg</a>. This data contains basic "
        f"information about hawker centres, and their cleaning dates.",
        f"\t• My data is updated every week so that I can get the latest information about hawker centres.",
        f"\t• MRT/LRT information can be found at <a "
        f"href=\"https://datamall.lta.gov.sg/content/datamall/en/static-data.html\">DataMall</a> by LTA. This data "
        f"contains the names, numbers and coordinates of MRT/LRT stations.",
        f"\t• My code can be found in this <a href=\"https://github.com/darensin01/SGHawkerBot\">Github repo</a>.",
        f"\t• My picture is from <a href=\"https://www.flaticon.com/authors/iconixar\">flaticon.com</a>."
    ]

    reply_text = "\n".join(reply_text)
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=reply_text,
                             parse_mode=telegram.ParseMode.HTML,
                             disable_web_page_preview=True, )


def get_date_today():
    date_today = datetime.today()
    return date_today


def get_date_today_str(fmt="%d/%m/%Y"):
    return get_date_today().strftime(fmt)


def hawkers_washing_today():
    date_today = get_date_today()
    quarter = pd.Timestamp(date_today).quarter

    qtr_start_col = f"q{quarter}_start"
    qtr_end_col = f"q{quarter}_end"

    # Get hawkers that are cleaning as of today
    cleaning_hawkers = hawker_data_df[
        (hawker_data_df[qtr_start_col] <= date_today) & (hawker_data_df[qtr_end_col] >= date_today)]

    # Rename start and end date for easy reference by other functions
    cleaning_hawkers = cleaning_hawkers[[qtr_start_col, qtr_end_col]].rename(columns={
        qtr_start_col: "start_date",
        qtr_end_col: "end_date"
    })

    # Format for printing
    cleaning_hawkers['start_date'] = cleaning_hawkers['start_date'].apply(lambda dt: dt.strftime("%d/%m"))
    cleaning_hawkers['end_date'] = cleaning_hawkers['end_date'].apply(lambda dt: dt.strftime("%d/%m"))

    return cleaning_hawkers


def hawkers_not_existing():
    return list(hawker_data_df[hawker_data_df['not_existing']].index)


def append_closed_hawker_info(hawker_name):
    closed_text = ""
    if hawker_name in washing_hawkers_df.index:
        row = washing_hawkers_df.loc[hawker_name]
        start_date = row['start_date']
        end_date = row['end_date']
        closed_text += f"\t\t * <b>Closed</b> from {start_date} to {end_date}\n"

    if hawker_name in closed_hawkers:
        hawker_status = hawker_data_df.loc[hawker_name]['hawker_status']
        closed_text += f"\t\t * <b>{hawker_status}</b>\n"

    return closed_text


def search_hawker_by_name(update, context):
    log_command_pressed(update)

    all_hawker_names = hawker_data_df.index.unique()
    query = ' '.join(context.args)
    logging.info(f"Received query for hawker name search: {query}")

    fuzz_name = [(fuzz.partial_ratio(n, query), n) for n in all_hawker_names]
    sorted_name_fuzz = sorted(fuzz_name, reverse=True)

    top_10_names = [name for score, name in sorted_name_fuzz][:10]
    reply_text = f"<i>Here are the top {RESULTS_TO_SHOW} hawker centres that match your query </i>{query}. " \
                 f"<i>Click on each link to open its location.</i>\n\n"

    list_of_emojis = get_shuffled_emojis()
    for idx, hawker_name in enumerate(top_10_names):
        hawker_gmaps_url = hawker_data_df.loc[hawker_name]['hawker_gmaps_url']
        reply_text += f"{list_of_emojis[idx]} <a href='{hawker_gmaps_url}'>{hawker_name}</a>\n"
        reply_text += append_closed_hawker_info(hawker_name)

    context.bot.send_message(chat_id=update.message.chat_id,
                             parse_mode=telegram.ParseMode.HTML,
                             text=reply_text,
                             disable_web_page_preview=True)


def search_hawker_by_mrt(update, context):
    log_command_pressed(update)

    search_df = hawker_mrt_dist_df.copy()
    unique_station_names_nums = search_df[['station_name_cleaned', 'station_num']].drop_duplicates()
    unique_station_names_nums['first_letter'] = unique_station_names_nums['station_name_cleaned'].apply(lambda s: s[0])

    sorted_unique_first_letters = sorted(unique_station_names_nums['first_letter'].unique())

    keyboard = []
    for letter in sorted_unique_first_letters:
        button_text = f"{letter.upper()}"
        keyboard.append([telegram.InlineKeyboardButton(button_text, callback_data=letter)])

    reply_markup = telegram.InlineKeyboardMarkup(keyboard)
    update.message.reply_text('🚉 Select the first letter of an MRT/LRT station:', reply_markup=reply_markup)
    return MRT_FIRST


def selected_mrt_station_letter(update, context):
    query = update.callback_query
    first_letter = query.data

    search_df = hawker_mrt_dist_df.copy()
    stations = search_df[['station_name_cleaned', 'station_num']].drop_duplicates()
    selected_stations = stations[stations['station_name_cleaned'].apply(lambda s: s[0] == first_letter)]
    sorted_station_names_nums = selected_stations.sort_values(by="station_name_cleaned")

    keyboard = []
    for idx, row in sorted_station_names_nums.reset_index(drop=True).iterrows():
        station_name = row['station_name_cleaned']
        station_num = row['station_num']
        button_text = f"{station_name.title()} ({station_num})"
        keyboard.append([telegram.InlineKeyboardButton(button_text, callback_data=station_num)])

    reply_markup = telegram.InlineKeyboardMarkup(keyboard)
    query.edit_message_text(text=f"🚉 Select an MRT/LRT station that starts with {first_letter.upper()}:",
                            reply_markup=reply_markup)
    return MRT_SECOND


def selected_mrt_station(update, context):
    query = update.callback_query
    query.answer()

    search_df = hawker_mrt_dist_df.copy()
    unique_stations = search_df[['station_name_cleaned', 'station_num']].drop_duplicates()
    unique_stations = unique_stations.set_index("station_num")

    selected_station_num = query.data
    selected_station_name = unique_stations.loc[selected_station_num]['station_name_cleaned']

    user_update_text = f"Searching for hawkers near <i>{selected_station_name.title()} ({selected_station_num})</i>"
    query.edit_message_text(parse_mode=telegram.ParseMode.HTML, text=user_update_text)

    search_df = search_df[search_df['station_num'] == selected_station_num]
    search_df = search_df.sort_values(by="distance", ascending=True).iloc[:RESULTS_TO_SHOW].reset_index(drop=True)

    reply_text = f"<i>Here are the {RESULTS_TO_SHOW} hawker centres nearest to {selected_station_name.title()}"\
        f" ({selected_station_num}). Click on each link to open its location on Google Maps</i>\n\n"

    list_of_emojis = get_shuffled_emojis()
    for idx, row in search_df.iterrows():
        dist = round(row['distance'], 1)
        hawker_name = row['hawker_name']
        gmaps_url = hawker_data_df.loc[hawker_name]['hawker_gmaps_url']
        reply_text += f"{list_of_emojis[idx]} ({dist} km) <a href='{gmaps_url}'>{hawker_name}</a>\n"
        reply_text += append_closed_hawker_info(hawker_name)

    query.edit_message_text(parse_mode=telegram.ParseMode.HTML,
                            text=reply_text,
                            disable_web_page_preview=True)


def search_hawker_by_location_prompt(update, context):
    reply_text = "To list the hawker centres near you, simply send your location!"
    context.bot.send_message(chat_id=update.effective_chat.id, text=reply_text)


def search_hawker_by_location(update, context):
    log_command_pressed(update)

    user_location = update.message.location
    user_coords = (user_location.latitude, user_location.longitude)

    search_df = hawker_data_df.copy()

    search_df['dist_from_user'] = search_df['hawker_coords'] \
        .apply(lambda hc: geopy.distance.distance(hc, user_coords).km)

    search_df = search_df.sort_values(by="dist_from_user").iloc[:RESULTS_TO_SHOW]

    reply_text = f"<i>Here are the top {RESULTS_TO_SHOW} hawker centres near you."\
                 "Click on each link to open its location</i>\n\n"

    list_of_emojis = get_shuffled_emojis()
    for idx, (hawker_name, row) in enumerate(search_df.iterrows()):
        dist = round(row['dist_from_user'], 1)
        gmaps_url = row['hawker_gmaps_url']
        reply_text += f"{list_of_emojis[idx]} <a href='{gmaps_url}'>{hawker_name}</a> ({dist} km)\n"
        reply_text += append_closed_hawker_info(hawker_name)

    context.bot.send_message(chat_id=update.message.chat_id,
                             parse_mode=telegram.ParseMode.HTML,
                             text=reply_text,
                             disable_web_page_preview=True)


def list_hawker_cleaning(update, context):
    log_command_pressed(update)

    if len(closed_hawkers) + len(washing_hawkers_df) <= 0:
        reply_text = "No hawkers are closed today!"
    else:
        formatted_date = get_date_today_str("%d/%m/%Y")
        reply_text = f"<i>The following hawker centres are closed today ({formatted_date}):</i>\n\n"

        if len(washing_hawkers_df) > 0:
            reply_text += "<b>🧹 Hawkers cleaning:</b>\n"
            for idx, (hawker_name, row) in enumerate(washing_hawkers_df.iterrows()):
                start_date = row['start_date']
                end_date = row['end_date']
                reply_text += f"{idx + 1}) {hawker_name}\n\t * {start_date} to {end_date}\n"

        if len(closed_hawkers) > 0:
            reply_text += "\n<b>🏗 Hawkers closed:</b>\n"
            for idx, h_name in enumerate(closed_hawkers):
                status = hawker_data_df.loc[h_name]['hawker_status']
                reply_text += f"{idx + 1}) {h_name} ({status})\n"

    context.bot.send_message(chat_id=update.message.chat_id,
                             parse_mode=telegram.ParseMode.HTML,
                             text=reply_text,
                             disable_web_page_preview=True)


def get_json_data_from_url(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        sys.exit(-1)


def handle_stickers(update, context):
    context.bot.send_message(chat_id=update.effective_chat.id,
                             text=update.message.sticker.emoji)


def unknown(update, context):
    log_command_pressed(update)

    context.bot.send_message(chat_id=update.effective_chat.id,
                             text="Sorry, I didn't understand that. Try /start to see a list of instructions.")


RESULTS_TO_SHOW = 10
LOGS_DIR = "./logs"
PERSISTENCE_DIR = "./persistence"
EMOJIS = [
    "🍲",  # pot of food
    "🥗",  # salad
    "🥤",  # cup with straw
    "🍜",  # steaming bowl
    "🍱",  # bento
    "🥞",  # pancakes
    "🦞",  # lobster
    "☕",  # hot beverage
    "🥮",  # mooncake
    "🍚",  # cooked rice
]

if not os.path.isdir(LOGS_DIR):
    os.mkdir(LOGS_DIR)

if not os.path.isdir(PERSISTENCE_DIR):
    os.mkdir(PERSISTENCE_DIR)

logging.basicConfig(filename=os.path.join(LOGS_DIR, "{}.log".format(get_date_today_str("%d%m%Y"))),
                    filemode='a',
                    datefmt="%H:%M:%S",
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.info(f"Today's date: {get_date_today_str()}")

# Data preparation section
with open("token.txt", 'r') as f:
    token = f.read()

hawker_mrt_dist_json = get_json_data_from_url("https://raw.githubusercontent.com/darensin01/SGHawkerBot/main"
                                              "/mrt_hawker_distances.json")
hawker_mrt_dist_df = pd.DataFrame.from_records(hawker_mrt_dist_json)
logging.info(f"Number of entries in MRT hawker distance DF: {len(hawker_mrt_dist_df)}")

hawker_data_json = get_json_data_from_url("https://raw.githubusercontent.com/darensin01/SGHawkerBot/main/hawker_data"
                                          ".json")
hawker_data_df = pd.DataFrame.from_records(hawker_data_json)
hawker_data_df['not_existing'] = hawker_data_df['hawker_status'].apply(lambda s: "existing" not in s.lower())
hawker_data_df = hawker_data_df.set_index("hawker_name")
logging.info(f"Number of entries in hawker information DF: {len(hawker_data_df)}")

for c in ["q1_start", "q1_end", "q2_start", "q2_end", "q3_start", "q3_end", "q4_end"]:
    hawker_data_df[c] = hawker_data_df[c].apply(lambda d: datetime.strptime(d, "%d/%m/%Y"))

washing_hawkers_df = hawkers_washing_today()
closed_hawkers = hawkers_not_existing()
logging.info(f"Number of hawkers washing today: {len(washing_hawkers_df)}")
logging.info(f"Number of hawkers closed today: {len(closed_hawkers)}")

# Start of handler/dispatcher code
bot = telegram.Bot(token=token)

user_persistence = PicklePersistence(filename=os.path.join(PERSISTENCE_DIR, "user_persistence"))

updater = Updater(token=token, use_context=True, persistence=user_persistence)
dispatcher = updater.dispatcher
dispatcher.add_handler(CommandHandler('start', start))
dispatcher.add_handler(CommandHandler('closed', list_hawker_cleaning))
dispatcher.add_handler(CommandHandler('search', search_hawker_by_name))
dispatcher.add_handler(CommandHandler('info', get_info))

MRT_FIRST, MRT_SECOND = range(2)
mrt_conv_handler = ConversationHandler(
    entry_points=[CommandHandler('mrt', search_hawker_by_mrt)],
    states={
        MRT_FIRST: [telegram.ext.CallbackQueryHandler(selected_mrt_station_letter)],
        MRT_SECOND: [telegram.ext.CallbackQueryHandler(selected_mrt_station)]
    },
    per_message=False,
    fallbacks=[CommandHandler('mrt', search_hawker_by_mrt)]
)
dispatcher.add_handler(mrt_conv_handler)

dispatcher.add_handler(CommandHandler('nearest', search_hawker_by_location_prompt))
location_handler = MessageHandler(Filters.location & (~Filters.command), search_hawker_by_location)
dispatcher.add_handler(location_handler)

sticker_handler = MessageHandler(Filters.sticker & (~Filters.command), handle_stickers)
dispatcher.add_handler(sticker_handler)

random_text_handler = MessageHandler(Filters.text & (~Filters.command), start)
dispatcher.add_handler(random_text_handler)

# Must be added last
unknown_handler = MessageHandler(Filters.command, unknown)
dispatcher.add_handler(unknown_handler)

updater.start_polling()
updater.idle()
