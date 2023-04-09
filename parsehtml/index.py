import boto3
import ydb
import os
import json

import logging

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
driver = ydb.Driver(
    endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'),
    credentials=ydb.iam.MetadataUrlCredentials(),)
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

def get_cards_travelata(soup: BeautifulSoup) -> List[dict]:
    return soup.find_all("div", class_="serpHotelCard")

@parse_func_wrapper("https://travelata.ru/")
def parse_hotel_card_travelata(card: BeautifulSoup) -> dict:
    title, href = get_text(card, "a", "serpHotelCard__title", attrs=["href"])
    location = get_text(card, "a", class_="serpHotelCard__resort")

    distances_card = card.find("div", class_="serpHotelCard__distances")
    if distances_card is not None:
        distances = list(
            map(
                lambda x: x.get_text(" ", strip=True),
                distances_card.find_all("div", class_="serpHotelCard__distance"),
            )
        )
    else:
        distances = []

    distances_str = ';'.join(distances)

    rating = get_text(card, "div", "serpHotelCard__rating")
    reviews = get_text(card, "a", "hotel-reviews", raise_error=False)
    less_places = get_text(
        card, "div", "serpHotelCard__tip__less-places", raise_error=False
    )
    num_stars = len(card.find_all("i", "icon-i16_star"))

    orders_count = get_text(
        card, "div", "serpHotelCard__ordersCount", raise_error=False
    )
    criteria = get_text(card, "div", "serpHotelCard__criteria")
    price = get_text(card, "span", "serpHotelCard__btn-price")
    oil_tax = get_text(card, "span", "serpHotelCard__btn-oilTax")

    attributes_cards = card.find_all("div", class_="serpHotelCard__attribute")
    attributes = ";".join(map(lambda x: x.get_text(" ", strip=True), attributes_cards))

    return {
        "title": title,
        "href": href,
        "location": location,
        "distances": distances_str,
        "rating": rating,
        "reviews": reviews,
        "less_places": less_places,
        "num_stars": num_stars,
        "orders_count": orders_count,
        "criteria": criteria,
        "price": price,
        "oil_tax": oil_tax,
        "attributes": attributes,
    }

def load_process_html_cards_from_s3(
    client, Bucket: str, Key: str,
    get_cards: Callable, parse_card: Callable
) -> list:
    prefix = "/".join(Key.split("/")[:-1])
    object_key = os.path.join(prefix, "content.html")
    meta_key = os.path.join(prefix, "meta.json")

    meta_object_response = client.get_object(
        Bucket=Bucket, Key=meta_key)

    meta = json.loads(meta_object_response["Body"].read())

    if not meta["failed"]:
        logging.info("Start parsing object")
        get_object_response = client.get_object(
            Bucket=Bucket, Key=object_key)
        content = get_object_response["Body"].read()
        soup = BeautifulSoup(content, "html.parser")
        cards = get_cards(soup)
        result = list(map(parse_card, cards))
        logging.info("End parsing object")
        result_with_meta = update_dicts(
            result, parsing_id=meta["parsing_id"], key=Key, bucket=Bucket)
        
        logging.info("Deleting objects")
        client.delete_object(Bucket=Bucket, Key=Key)
        client.delete_object(Bucket=Bucket, Key=object_key)
        client.delete_object(Bucket=Bucket, Key=meta_key)
        return result_with_meta
    else:
        logging.error("Failed flg in meta")
        client.delete_object(Bucket=Bucket, Key=Key)
        client.delete_object(Bucket=Bucket, Key=meta_key)
        return None

def format_record(record: list):
    return '''("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}",
    "{}","{}",cast("{}" as datetime),"{}","{}","{}","{}","{}","{}","{}")'''.format(*record.values())

def create_statement(data: list) -> str:
    query = """
    REPLACE INTO `parser/raw/travelata`(
        title, href, location, distances, 
        rating, reviews, less_places, 
        num_stars, orders_count, criteria,
        price, oil_tax, attributes, created_dttm,
        website, link, offer_hash, row_id, parsing_id, 
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
        get_cards_travelata, parse_hotel_card_travelata
    )

    if result is None:
        return 0

    queries = create_queries(result, max_records=5)
    for query in queries:
        try:
            pool.retry_operation_sync(create_execute_query(query))
        except:
            raise ValueError(f"Failed at query {query}")
        
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