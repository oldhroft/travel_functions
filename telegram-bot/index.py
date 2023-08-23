import boto3
import os
import json
import datetime
from typing import Union

import telegram.ext
from telegram.ext import Dispatcher
from telegram import Update, Bot, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext.callbackcontext import CallbackContext
from telegram.ext.commandhandler import CommandHandler
from telegram.ext import CallbackQueryHandler

from telegram_bot_calendar import DetailedTelegramCalendar, LSTEP

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
    line0 = f'{result["title"]}\n'
    line1 = f'Рейтинг {int(result["num_stars"]) * STAR}\n'

    line2 = f'На {int(result["num_nights"])} ночей, с {result["start_date"]} до {result["end_date"]}\n'
    line3 = f'{result["country_name"]}, {result["city_name"]}\n'
    line4 = f'Стоимость {result["price"]} RUB\n'

    if result["mealplan"] != "" and result["mealplan"] is not None:
        line5 = f'Тип питания {result["mealplan"]} RUB\n'
    else:
        line5 = ""

    if result["room_type"] != "" and result["room_type"] is not None:
        line6 = f'Тип комнаты {result["room_type"]} RUB\n'
    else:
        line6 = ""

    price_change = result["price_change"]
    price = result["price"]

    price_ratio = price - price_change
    price_ratio = price_change / price

    if price_ratio <= -0.01:
        ratio_percent = price_ratio * 100
        line7 =f"🔻Подешевело на {ratio_percent:.1f}%\n"
    else:
        line7 = ""

    text = line0 + line1 + line2 + line3 + line4 + line5 + line6 + line7 + result["link"]

    return text

def get_offers_handler(data):
    url = os.getenv("GET_OFFERS_HANDLER")
    response = requests.get(url, json=data)
    try:
        results = response.json()
    except:
        logging.info(response.content)
        raise ValueError("JSON decode error")
    if len(results) == 0:
        return ["Ничего не найдено"]
    logging.info(json.dumps(results))
    return list(map(format_result, results))

def button(update: Update, context: CallbackContext) -> None:
    """Parses the CallbackQuery and updates the message text."""
    query = update.callback_query

    logging.info("Fetch callback", extra={"context": {"SEVERITY": "info"}})

    # CallbackQueries need to be answered, even if no notification to the user is needed
    # Some clients may have trouble otherwise. See https://core.telegram.org/bots/api#callbackquery
    query.answer()

    if query.data.startswith("cbcal"):
        min_date = datetime.date.today() + datetime.timedelta(1)
        max_date = min_date + datetime.timedelta(29)
        result, key, step = DetailedTelegramCalendar(
            min_date=min_date, max_date=max_date).process(query.data)
        if not result and key:

            logging.info(f"Select {LSTEP[step]}")

            if LSTEP[step] == 'year':
                text = "год"
            elif LSTEP[step] == 'month':
                text = "месяц"
            elif LSTEP[step] == 'day':
                text = 'день'
            query.edit_message_text(f"Пожалуйста, выбери примерный {text} вылета",
                                    reply_markup=key)
        elif result:
            query.edit_message_text(f"Примерная дата вылета {result}")
            logging.info(f"Selected {result}")
            keyboard_list = [[]]
            j = 0
            for days in range(0, 30):
                entry = json.dumps({"val": days, "id": 2})
                if days % 5 == 0:
                    j += 1
                    keyboard_list.append([])

                keyboard_list[j].append(
                    InlineKeyboardButton(str(days), callback_data=entry))
        
            reply_markup = InlineKeyboardMarkup(keyboard_list)
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text= "Выбери диапазон дней от даты вылета\nесли дата вылета точная, выбери 0",
                reply_markup=reply_markup)
            logging.info("Showing interval options", extra={"context": {"SEVERITY": "info"}})

            params = {
                "min_departure_date": str(result)
            }

            user = update.to_dict()["callback_query"]["from"]

            user_event = {
                "user": user,
                "event": "select_param",
                "clear": False,
                "param": json.dumps(params)
            }

            post_user_event(user_event)
        
        # DO NOT REMOVE
        return


    data = json.loads(query.data)

    logging.info(f"Got callback: {json.dumps(data)}")

    if data["id"] == 1:
        query.edit_message_text(text=countries_dict_tg.get(data["val"]))

        logging.info("Edit msg country", extra={"context": {"SEVERITY": "info"}})

        min_date = datetime.date.today() + datetime.timedelta(1)
        max_date = min_date + datetime.timedelta(29)

        calendar, step = DetailedTelegramCalendar(min_date=min_date, max_date=max_date).build()

        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Выбери примерную дату вылета",
            reply_markup=calendar)
        
        params = {
            "country_name": countries_dict.get(data["val"])
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }

        post_user_event(user_event)

    elif data["id"] == 2:
        text_mn = f"Диапазон дней от даты вылета: {data['val']}"
        query.edit_message_text(text=text_mn)

        logging.info(f"Edit msg interval")
        keyboard_list = []
        for num_nights in range(5, 9):
            entry = json.dumps({"val": num_nights, "id": 3})
            keyboard_list.append(
                InlineKeyboardButton(str(num_nights), callback_data=entry))
    
        reply_markup = InlineKeyboardMarkup([keyboard_list])
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text= "Отлично👌🏻\nТеперь выбери минимальное количество ночей",
            reply_markup=reply_markup)
        logging.info("Showing nights options", extra={"context": {"SEVERITY": "info"}})

        params = {
            "interval_days": data["val"]
        }

        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "select_param",
            "clear": False,
            "param": json.dumps(params)
        }

        post_user_event(user_event)
    
    elif data["id"] == 3:
        text_mn = f"Минимальное число ночей: {data['val']}"
        query.edit_message_text(text=text_mn)
        logging.info("Edit msg nights", extra={"context": {"SEVERITY": "info"}})
        min_nights = data["val"]
        keyboard_list = []
        for num_nights in range(min_nights, 9):
            entry = json.dumps({"val": num_nights, "id": 4})
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
    
    elif data["id"] == 4:
        text_mn = f"Максимальное число ночей: {data['val']}"
        query.edit_message_text(text=text_mn)
        logging.info("Edit msg min nights", extra={"context": {"SEVERITY": "info"}})

        keyboard_list = []
        for num_stars in range(0, 6):
            entry = json.dumps({"val": num_stars, "id": 5})
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

    elif data["id"] == 5:

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

        data = {
            "user_id": user["id"],
            "params": all_params,
            "offset": 0,
            "number": 4
        }
        logging.info("Sending info to get offers")
        texts = get_offers_handler(data)
        logging.info("Start displaying", extra={"context": {"SEVERITY": "info"}})
        
        for text in texts[:-1]:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text)
            
        entry = json.dumps({"val": 0, "id": 6})
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Загрузить еще", callback_data=entry)]
        ])
        logging.info(f"Last tex {texts[-1]}")
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=texts[-1], reply_markup=reply_markup)
        
        logging.info("End displaying", extra={"context": {"SEVERITY": "info"}})

    elif data["id"] == 6:
        query.edit_message_text(text="Загружаем еще...")
        user = update.to_dict()["callback_query"]["from"]

        user_event = {
            "user": user,
            "event": "scroll",
            "clear": False,
            "param": None
        }
        post_user_event(user_event)

        all_params = get_params(user["id"])
        logging.info("quering...", extra={"context": {"SEVERITY": "info"}})
        offset = data["val"] + 4
        logging.info(f"Offset {offset}")
        data = {
            "user_id": user["id"],
            "params": all_params,
            "offset": offset,
            "number": 4
        }
        
        logging.info("Sending info to get offers")
        texts = get_offers_handler(data)
        logging.info("Start displaying", extra={"context": {"SEVERITY": "info"}})
        entry = json.dumps({"val": offset, "id": 6})

        if len(texts) < 4:
            return
        
        for text in texts[:-1]:
            context.bot.send_message(
                chat_id=update.effective_chat.id,
                text=text)
            
        reply_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("Загрузить еще", callback_data=entry)]
        ])
        context.bot.send_message(
            chat_id=update.effective_chat.id,
            text=texts[-1], reply_markup=reply_markup)
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