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

import datetime
import ydb
import ydb.iam

import requests

def serial_date_to_string(srl_no):
    new_date = datetime.datetime(1970, 1, 1, 0, 0) + datetime.timedelta(srl_no - 1)
    return new_date.strftime("%d.%m.%Y")

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
LIMIT 1;
"""

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
    update.message.reply_text(hello_string)

def help_(update: Update, context: CallbackContext) -> int:
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

    keyboard_list = []
    for i, country in countries_dict_tg.items():
        entry = json.dumps({"val": i , "id": 1})
        keyboard_list.append(
            InlineKeyboardButton(country, callback_data=entry))

    reply_markup = InlineKeyboardMarkup([keyboard_list])

    update.message.reply_text(
        "Выбери страну",
        reply_markup=reply_markup
    )
    post_user_event(update.to_dict())

def query_offer(params: dict) -> str:
    country = params["country"]
    if country is None:
        query_country = "1=1"
    else:
        query_country = f"String::Strip(country_name) = '{country}'"
    
    where_query = query_country
    query = query_template.format(where_query=where_query)
    session = initialize_session()
    result = session.transaction().execute(query, commit_tx=True)[0].rows
    if len(result) == 0:
        return "Ничего не найдено"
    
    result = result[0]
    line0 = f"{result.title}\n"
    line1 = f"Рейтинг {int(result.num_stars) * STAR}\n"

    line2 = f"На {int(result.num_nights)} ночей, с {result.start_date} до {result.end_date}\n"
    line3 = f"{result.country_name}, {result.city_name}\n"
    line4 = f"Стоимость {result.price} RUB\n"
    text = line0 + line1 + line2 + line3 + line4 + result.link
    return text


def button(update: Update, context: CallbackContext) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    query.answer()

    data = json.loads(query.data)

    if data["id"] == 1:
        # Previous selection was selection of country
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Отлично👌🏻\nПодожди, пока я подберу варианты")
        
        query.edit_message_text(text=countries_dict_tg.get(data["val"]))

        params = {
            "country": countries_dict.get(data["val"])
        }

        text = query_offer(params)
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=text)


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