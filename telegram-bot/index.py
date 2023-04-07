import boto3
import os
import json
from typing import Union

import telegram.ext
from telegram.ext import Dispatcher
from telegram import Update, Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext import CallbackQueryHandler

import ydb
import ydb.iam

import requests

import logging

logging.getLogger().setLevel(logging.INFO)


def initialize_session():
    driver = ydb.Driver(
        endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'),
        credentials=ydb.iam.MetadataUrlCredentials(),)
    
    try:
        driver.wait(fail_fast=True, timeout=5)
        session = driver.table_client.session().create()
        return session
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)

def post_user_event(event):
    url = os.getenv("ADD_USER_HANDLER")
    requests.post(url, json=event)

help_string = """Вот что я могу:
/search - Найти туры
/start - Начать разговор
/help - Вывести список доступных команд
"""

query_template = """$format = DateTime::Format("%d.%m.%Y");

SELECT cast($format(start_date) as utf8) as start_date, 
    cast($format(end_date) as utf8) as end_date,
    title,
    country_name,
    num_nights,
    city_name,
    price,
    link,
    num_stars
FROM `parser/prod/offers`
WHERE {where_query}
ORDER BY price / num_nights ASC
LIMIT 4;
"""

query_params_template = """select param
from `users/events`
where user_id = {user_id}
    and event = 'select_param'"""


hello_string = """Привет!👋
Я помогу подобрать удобные туры
Используй /search чтобы подобрать тур
"""

def load_to_s3(data: Union[str, dict, list], Key, Bucket, is_json=False):
    boto_session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3 = boto_session.client(
        service_name="s3",
        endpoint_url=os.environ["AWS_ENDPOITNT_URL"],
        region_name=os.environ["AWS_REGION_NAME"],
    )

    if isinstance(data, list) or isinstance(data, dict):
        if is_json:
            data = json.dumps(data)
        else:
            raise ValueError("You should explicitly specify is_json option")
    elif isinstance(data, str):
        pass
    else:
        raise ValueError("data should be str, list or dict")
    s3.put_object(Body=data, Bucket=Bucket, Key=Key)

def start(update: Update, context: CallbackContext) -> int:
    logging.info("Start event", extra={"context": {"SEVERITY": "info"}})
    update.message.reply_text(hello_string)

def help_(update: Update, context: CallbackContext) -> int:
    logging.info("Help event", extra={"context": {"SEVERITY": "info"}})
    update.message.reply_text(help_string)


countries_dict = {
    0: "ОАЭ",
    1: "Таиланд",
    2: "Египет",
    3: None
}

countries_dict_tg = {
    0: "ОАЭ🇦🇪",
    1: "Таиланд🇹🇭",
    2: "Египет🇪🇬",
    3: "Любая"
}

STAR = "⭐"

def search(update: Update, context: CallbackContext) -> int:

    logging.info("Search event", extra={"context": {"SEVERITY": "info"}})
    keyboard_list = []
    for i, country in countries_dict_tg.items():
        entry = json.dumps({"val": i , "id": 1})
        keyboard_list.append(
            InlineKeyboardButton(country, callback_data=entry))

    reply_markup = InlineKeyboardMarkup([keyboard_list])

    user_event = {
        "user": update.to_dict()["message"]["from"],
        "event": "search",
        "clear": True
    }

    post_user_event(user_event)
    logging.info("Showing reply markup choose country", extra={"context": {"SEVERITY": "info"}})

    update.message.reply_text(
        "Выбери страну",
        reply_markup=reply_markup
    )

from collections import ChainMap

def get_params(user_id):
    query_params = query_params_template.format(user_id=user_id)
    session = initialize_session()

    results = session.transaction().execute(query_params, commit_tx=True)[0].rows

    # Double loading because of string escaping
    params = [
        json.loads(entry.param) for entry in results
    ]
    return dict(ChainMap(*params))


def format_result(result):
    line0 = f"{result.title}\n"
    line1 = f"Рейтинг {int(result.num_stars) * STAR}\n"

    line2 = f"На {int(result.num_nights)} ночей, с {result.start_date} до {result.end_date}\n"
    line3 = f"{result.country_name}, {result.city_name}\n"
    line4 = f"Стоимость {result.price} RUB\n"
    text = line0 + line1 + line2 + line3 + line4 + result.link
    return text


def query_offer(params: dict) -> str:
    country = params["country"]
    min_nights = params["min_nights"]
    max_nights = params["max_nights"]
    num_stars = params["num_stars"]
    if country is None:
        query_country = "1=1"
    else:
        query_country = f"String::Strip(country_name) = '{country}'"
    
    query_nights = f" AND num_nights >= {min_nights} AND num_nights <= {max_nights} "

    query_stars = f" AND num_stars >= {num_stars}"

    where_query = query_country + query_nights + query_stars
    query = query_template.format(where_query=where_query)
    session = initialize_session()
    results = session.transaction().execute(query, commit_tx=True)[0].rows
    if len(results) == 0:
        return ["Ничего не найдено"]
    
    return list(map(format_result, results))

def button(update: Update, context: CallbackContext) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    logging.info("Fetch callback", extra={"context": {"SEVERITY": "info"}})

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    query.answer()

    data = json.loads(query.data)

    if data["id"] == 1:
        query.edit_message_text(text=countries_dict_tg.get(data["val"]))

        logging.info("Edit msg country", extra={"context": {"SEVERITY": "info"}})
        # Previous selection was selection of country
        keyboard_list = []
        for num_nights in range(5, 9):
            entry = json.dumps({"val": num_nights, "id": 2})
            keyboard_list.append(
                InlineKeyboardButton(str(num_nights), callback_data=entry))
    
        reply_markup = InlineKeyboardMarkup([keyboard_list])
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Отлично👌🏻\nТеперь выбери минимальное количество ночей",
            reply_markup=reply_markup)
        logging.info("Showing nights options", extra={"context": {"SEVERITY": "info"}})
        
        params = {
            "country": countries_dict.get(data["val"])
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }

        post_user_event(user_event)
    
    if data["id"] == 2:
        text_mn = f"Минимальное число ночей: {data['val']}"
        query.edit_message_text(text=text_mn)
        logging.info("Edit msg nights", extra={"context": {"SEVERITY": "info"}})
        min_nights = data["val"]
        keyboard_list = []
        for num_nights in range(min_nights, 9):
            entry = json.dumps({"val": num_nights, "id": 3})
            keyboard_list.append(
                InlineKeyboardButton(str(num_nights), callback_data=entry))
        
        reply_markup = InlineKeyboardMarkup([keyboard_list])
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Теперь выбери максимальное количество ночей",
            reply_markup=reply_markup)
        logging.info("Showing max nights options", extra={"context": {"SEVERITY": "info"}})
        
        params = {
            "min_nights": data["val"]
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }
        post_user_event(user_event)
    
    if data["id"] == 3:
        text_mn = f"Максимальное число ночей: {data['val']}"
        query.edit_message_text(text=text_mn)
        logging.info("Edit msg min nights", extra={"context": {"SEVERITY": "info"}})

        keyboard_list = []
        for num_stars in range(0, 6):
            entry = json.dumps({"val": num_stars, "id": 4})
            if num_stars == 0:
                text = "Без звезд"
            else:
                text = num_stars * STAR
            keyboard_list.append(
                InlineKeyboardButton(text, callback_data=entry))
            
        reply_markup = InlineKeyboardMarkup([keyboard_list])
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Теперь минимальное количество звезд отеля",
            reply_markup=reply_markup)
        logging.info("Showing star options", extra={"context": {"SEVERITY": "info"}})
        
        params = {
            "max_nights": data["val"]
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }
        post_user_event(user_event)

    if data["id"] == 4:

        text_mn = f"Максимальное число звезд: {STAR * data['val']}"
        query.edit_message_text(text=text_mn)
        logging.info("Edit msg stars", extra={"context": {"SEVERITY": "info"}})

        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text="Отлично👌🏻\nПодожди, пока я подберу варианты")
        
        params = {
            "num_stars": data["val"]
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }
        post_user_event(user_event)
        logging.info("Getting params", extra={"context": {"SEVERITY": "info"}})
        all_params = get_params(user["id"])
        logging.info("quering...", extra={"context": {"SEVERITY": "info"}})
        texts = query_offer(all_params)
        logging.info("Start displaying", extra={"context": {"SEVERITY": "info"}})
        for text in texts:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text)
        
        logging.info("End displaying", extra={"context": {"SEVERITY": "info"}})


def handler(event, context):
    bot = Bot(os.environ["BOT_TOKEN"])
    dispatcher = Dispatcher(bot, None, use_context=True)
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_))
    dispatcher.add_handler(CallbackQueryHandler(button))
    dispatcher.add_handler(CommandHandler("search", search))

    message = json.loads(event["body"])
    load_to_s3(message, "message0.json", "parsing", is_json=True)
    dispatcher.process_update(
        Update.de_json(json.loads(event["body"]), bot)
    )

    return {
        'statusCode': 200,
        "message": message,
        'body': 'Hello World!',
    }