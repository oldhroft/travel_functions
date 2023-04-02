import boto3
import os
import json
from typing import Union

import telegram.ext
from telegram.ext import Dispatcher, MessageHandler, Filters
from telegram import Update, Bot
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler

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

query = """SELECT *
FROM `parser/prod/offers`
WHERE num_stars > 3
ORDER BY price / num_nights ASC
LIMIT 1;
"""

hello_string = """Привет!👋\nЯ помогу подобрать удобные туры!
Используй /search чтобы подобрать тур!
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
    update.message.reply_text(
        "Привет!👋\nЯ помогу подобрать удобные туры!"
    )

def help_(update: Update, context: CallbackContext) -> int:
    update.message.reply_text(help_string)

def search(update: Update, context: CallbackContext) -> int:
    update.message.reply_text(
        "Секундочку, подбираю для тебя варианты"
    )
    post_user_event(update.to_dict())
    session = initialize_session()
    result = session.transaction().execute(query, commit_tx=True)[0].rows[0]
    line1 = f"На {int(result.num_nights)} ночей, с {serial_date_to_string(result.start_date)} до {serial_date_to_string(result.end_date)}\n"
    line2 = f"{result.country_name}, {result.city_name}\n"
    line3 = f"Стоимость {result.price} RUB\n"
    text = line1 + line2 + line3 + result.link
    update.message.reply_text(text)

def handler(event, context):
    bot = Bot(os.environ["BOT_TOKEN"])
    dispatcher = Dispatcher(bot, None, use_context=True)
    dispatcher.add_handler(CommandHandler("start", start))
    dispatcher.add_handler(CommandHandler("help", help_))
    dispatcher.add_handler(CommandHandler("search", search))

    message = json.loads(event["body"])
    dispatcher.process_update(
        Update.de_json(json.loads(event["body"]), bot)
    )
    

    return {
        'statusCode': 200,
        "message": message,
        'body': 'Hello World!',
    }