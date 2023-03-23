import boto3
import ydb
import os
import json

boto_session = boto3.session.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
)

s3 = boto_session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    region_name='ru-central1'
)

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

from typing import Optional, Callable
from bs4 import BeautifulSoup


def get_text(
    soup: BeautifulSoup,
    element: str,
    class_: str,
    raise_error: bool = True,
    attrs: Optional[list] = None,
) -> str:
    card = soup.find(element, class_=class_)

    if card is None and raise_error:
        raise ValueError(f"Element {element} with class {class_} not found")
    elif card is None:
        if attrs is None:
            return None
        else:
            return None, *(None for _ in range(len(attrs)))
    else:
        text = card.get_text(" ", strip=True)
        if attrs is None:
            return text
        else:
            attr_value = (card.attrs[a] for a in attrs)
            return text, *attr_value


from functools import wraps
import datetime
from urllib.parse import urljoin
from hashlib import md5
import uuid


def parse_func_wrapper(website: str, time_fmt: str = "%Y-%m-%dT%H:%M:%SZ") -> Callable:
    def dec_outer(fn):
        @wraps(fn)
        def somedec_inner(*args, **kwargs):
            result = fn(*args, **kwargs)
            result["created_dttm"] = datetime.datetime.now().strftime(time_fmt)
            result["website"] = website
            result["link"] = urljoin(website, result["href"])
            result["offer_hash"] = md5(result["link"].encode()).hexdigest()
            result["row_id"] = str(uuid.uuid4())
            return result

        return somedec_inner

    return dec_outer

import os
from typing import List

def update_dicts(dicts: List[dict], **kwargs) -> List[dict]:
    return list(map(lambda x: {**x, **kwargs}, dicts))

def get_cards(soup: BeautifulSoup) -> List[dict]:
    return soup.find_all(
        "div", class_="hotel_point"
    )

@parse_func_wrapper("https://tourist.tez-tour.com/")
def parse_card(card: BeautifulSoup) -> dict:

    _, href = get_text(card, "a", class_="fav-detailurl", attrs=["href"])
    
    _, preview_img = get_text(card, "img", class_="preview", attrs=["src"])

    try:
        (
            location_name, hotel_id, hotel_rating, 
            hotel_rating_text, latitude, longitude, title
        ) = get_text(
            card, "div", class_="city-name",
            attrs=[
                "data-hotel-id", "data-hotel-rating", "data-hotel-rating-text",
                "data-lat", "data-lng", "data-title",
            ]
        )
    except:
        location_name = get_text(card, "div", class_="city-name")
        hotel_id = None
        hotel_rating = None
        hotel_rating_text = None
        latitude = None
        longitude = None
        title = None
    
    _, hint_text = get_text(
        card, "div", class_="clipped-text", attrs=["data-title"],
        raise_error=False
    )

    amenities = card.find_all(
        "h6", class_="hotel-amenities-item"
    )

    amenities_list = ";".join(list(map(
        lambda x: x.get_text(" ", strip=True),
        amenities
    )))

    departure_info = (
        card.find("div", class_="inline-visible")
        .find("div", class_="type")
        .get_text(" ", strip=True))

    mealplan = get_text(
        card, "div", class_="fav-mealplan"
    )

    room_type = get_text(
        card, "div", class_="fav-room"
    )
    _, currency, price = get_text(
        card, "a", class_="price-box", attrs=["data-currency", "data-price", ]
    )

    price_box = get_text(
        card, "div", class_="price-box-hint",
    )

    price_include = get_text(
        card, "ul", class_="price-include",
    )

    _, stars_class_list = get_text(card, "div", "hotel-star-box", attrs=["class"])

    stars_class = ";".join(stars_class_list)

    till_info = card.find_all("div", class_="type")[2].get_text(" ", strip=True)

    return {
        "href": href,
        "preview_img": preview_img,
        "location_name": location_name,
        "hotel_id": hotel_id,
        "hotel_rating": hotel_rating,
        "hotel_rating_text": hotel_rating_text,
        "latitude": latitude,
        "longitude": longitude,
        "title": title,
        "hint_text": hint_text,
        "amenities_list": amenities_list,
        "departure_info": departure_info,
        "mealplan": mealplan,
        "room_type": room_type,
        "currency": currency,
        "price": price,
        "price_box": price_box,
        "price_include": price_include,
        "till_info": till_info,
        "stars_class": stars_class
    }


def load_process_html_cards_from_s3(
    client, Bucket: str, Key: str,
    get_cards: Callable, parse_card: Callable
) -> list:
    prefix = "/".join(Key.split("/")[:-1])
    object_key = os.path.join(prefix, "content.html")
    meta_key = os.path.join(prefix, "meta.json")

    meta_object_response =client.get_object(
        Bucket=Bucket, Key=meta_key)

    meta = json.loads(meta_object_response["Body"].read())

    if not meta["failed"]:
        get_object_response = client.get_object(
            Bucket=Bucket, Key=object_key)
        content = get_object_response["Body"].read()
        soup = BeautifulSoup(content, "html.parser")
        cards = get_cards(soup)
        result = list(map(parse_card, cards))
        result_with_meta = update_dicts(
            result, parsing_id=meta["parsing_id"], key=Key, bucket=Bucket)
        
        client.delete_object(Bucket=Bucket, Key=Key)
        client.delete_object(Bucket=Bucket, Key=object_key)
        client.delete_object(Bucket=Bucket, Key=meta_key)
        return result_with_meta
    else:
        client.delete_object(Bucket=Bucket, Key=Key)
        client.delete_object(Bucket=Bucket, Key=meta_key)
        return None

def format_record(record: list):
    template = (
        '("{href}","{preview_img}","{location_name}",'
        '"{hotel_id}","{hotel_rating}","{hotel_rating_text}",'
        '"{latitude}","{longitude}","{title}","{hint_text}",'
        '"{amenities_list}","{departure_info}","{mealplan}",'
        '"{room_type}","{currency}","{price}","{price_box}",'
        '"{price_include}","{till_info}","{stars_class}",'
        'cast("{created_dttm}" as datetime), "{website}",'
        '"{link}", "{offer_hash}", "{row_id}", "{parsing_id}",'
        '"{key}", "{bucket}")'
    )
    return template.format(**record)

def create_statement(data: list) -> str:
    query = """
    REPLACE INTO `parser/raw/teztour` (
        href, preview_img, location_name, hotel_id, hotel_rating,
        hotel_rating_text,latitude,longitude,title,hint_text,
        amenities_list,departure_info,mealplan,room_type,currency,
        price,price_box,price_include,till_info,stars_class,
        created_dttm, website, link, offer_hash, row_id, parsing_id, 
        key, bucket)
    VALUES
    {}
    """.format(',\n'.join(map(format_record, data)))

    return query

def create_queries(data, max_records):

    queries = []
    for i in range(len(data) // max_records + 1):
        sub_data = data[i * max_records: (i + 1) * max_records]
        if len(sub_data) > 0:
            queries.append(
                create_statement(data[i * max_records: (i + 1) * max_records])
            )
    return queries

def create_execute_query(query):
  # Create the transaction and execute query.
    def _execute_query(session):
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return _execute_query

def process_file(Bucket, Key):
    result = load_process_html_cards_from_s3(
        s3, Bucket, Key, 
        get_cards, parse_card
    )

    if result is None:
        return 0

    queries = create_queries(result, max_records=3)
    for query in queries:
        try:
            pool.retry_operation_sync(create_execute_query(query))
        except Exception as e:
            raise ValueError(f"Failed with exception {e} at query {query}")
        
    return len(result)
    
def handler(event, context):

    length = process_file(
        Bucket=event["messages"][0]["details"]["bucket_id"],
        Key=event["messages"][0]["details"]["object_id"]
    )

    return {
        "objects": length,
        'statusCode': 200,
    }