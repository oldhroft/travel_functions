import boto3
import ydb
import os
import json

from typing import Optional, Callable, List
from bs4 import BeautifulSoup


def get_s3_client():
    boto_session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    client = boto_session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        region_name="ru-central1",
    )
    return client


def initialize_session():
    driver = ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.iam.MetadataUrlCredentials(),
    )

    try:
        driver.wait(fail_fast=True, timeout=5)
        return driver.table_client
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)


from functools import wraps
import datetime
import time
from urllib.parse import urljoin
from hashlib import md5
import uuid


def parse_func_wrapper(website: str) -> Callable:
    def dec_outer(fn):
        @wraps(fn)
        def somedec_inner(*args, **kwargs):
            result = fn(*args, **kwargs)
            result["created_dttm"] = int(time.mktime(datetime.datetime.now().timetuple()))
            result["website"] = website
            result["link"] = urljoin(website, result["href"])
            result["offer_hash"] = md5(result["link"].encode()).hexdigest()
            result["row_id"] = str(uuid.uuid4())
            return result

        return somedec_inner

    return dec_outer


import os


def update_dicts(dicts: List[dict], **kwargs) -> List[dict]:
    return list(map(lambda x: {**x, **kwargs}, dicts))


def load_process_html_cards_from_s3(
    client, Bucket: str, Key: str, get_cards: Callable, parse_card: Callable
) -> list:
    prefix = "/".join(Key.split("/")[:-1])
    object_key = os.path.join(prefix, "content.html")
    meta_key = os.path.join(prefix, "meta.json")

    meta_object_response = client.get_object(Bucket=Bucket, Key=meta_key)

    meta = json.loads(meta_object_response["Body"].read())

    if not meta["failed"]:
        get_object_response = client.get_object(Bucket=Bucket, Key=object_key)
        content = get_object_response["Body"].read()
        soup = BeautifulSoup(content, "html.parser")
        cards = get_cards(soup)
        result = list(map(parse_card, cards))
        result_with_meta = update_dicts(
            result, parsing_id=meta["parsing_id"], key=Key, bucket=Bucket
        )

        # client.delete_object(Bucket=Bucket, Key=Key)
        # client.delete_object(Bucket=Bucket, Key=object_key)
        # client.delete_object(Bucket=Bucket, Key=meta_key)
        return result_with_meta
    else:
        # client.delete_object(Bucket=Bucket, Key=Key)
        # client.delete_object(Bucket=Bucket, Key=meta_key)
        return None


import posixpath

class RawUtf8Table:
    table_name = "raw/table"
    default_fields = [
        "created_dttm",
        "website",
        "link",
        "offer_hash",
        "row_id",
        "parsing_id",
        "key",
        "bucket",
    ]

    custom_fields = []

    def __init__(
        self, base_dir: str, client: ydb.TableClient, max_records: int = 5
    ) -> None:
        self.client = client
        self.base_dir = base_dir
        self.path = os.path.join(base_dir, self.table_name)

        column_types = ydb.BulkUpsertColumns()
        for field in self.custom_fields:
            column_types.add_column(field, ydb.PrimitiveType.Utf8)

        self.column_types = (
            column_types.add_column("created_dttm", ydb.PrimitiveType.Datetime)
            .add_column("website", ydb.PrimitiveType.Utf8)
            .add_column("link", ydb.PrimitiveType.Utf8)
            .add_column("offer_hash", ydb.PrimitiveType.Utf8)
            .add_column("row_id", ydb.PrimitiveType.Utf8)
            .add_column("parsing_id", ydb.PrimitiveType.Utf8)
            .add_column("key", ydb.PrimitiveType.Utf8)
            .add_column("bucket", ydb.PrimitiveType.Utf8)
        )

    def bulk_upsert(self, data: List[dict]):
        self.client.bulk_upsert(self.path, data, self.column_types)


# Helpful utility, allows to extract text from html


def get_text(
    soup: BeautifulSoup,
    element: str,
    class_: str,
    raise_error: bool = True,
    attrs: Optional[list] = None,
) -> str:
    """Helpful utility, allows to extract text from soup html
    Params:
    soup: BeautifulSoup
        soup element from which text is extracted
    element: str
        name of an element, from which text should be extracted, i.e. div, a, i etc
    class_: str
        html class of an element, from which text should be extracted
    raise_error: bool
        whether to raise error if element is not found
    attrs: list of string
        if passed, function will also return these attributes parsed from element
    """

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


# ----------------
# Change from here
# ----------------


class RawTeztour(RawUtf8Table):
    """Raw table class

    To create your table, just change the name of the class,
    attribute table_name and add your custom fields
    all those fields should be string!

    """

    table_name = "raw/teztour"
    custom_fields = [
        "href",
        "preview_img",
        "location_name",
        "hotel_id",
        "hotel_rating",
        "hotel_rating_text",
        "latitude",
        "longitude",
        "title",
        "hint_text",
        "amenities_list",
        "departure_info",
        "mealplan",
        "room_type",
        "currency",
        "price",
        "price_box",
        "price_include",
        "till_info",
        "stars_class",
    ]


def get_cards(soup: BeautifulSoup) -> List[dict]:
    """functions to get hotel cards from soup, change it to match your case"""
    return soup.find_all("div", class_="hotel_point")


@parse_func_wrapper("https://tourist.tez-tour.com/")
def parse_card(card: BeautifulSoup) -> dict:
    """function to parse one hotel card, change it to match your case"""
    _, href = get_text(card, "a", class_="fav-detailurl", attrs=["href"])

    _, preview_img = get_text(card, "img", class_="preview", attrs=["src"])

    try:
        (
            location_name,
            hotel_id,
            hotel_rating,
            hotel_rating_text,
            latitude,
            longitude,
            title,
        ) = get_text(
            card,
            "div",
            class_="city-name",
            attrs=[
                "data-hotel-id",
                "data-hotel-rating",
                "data-hotel-rating-text",
                "data-lat",
                "data-lng",
                "data-title",
            ],
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
        card, "div", class_="clipped-text", attrs=["data-title"], raise_error=False
    )

    amenities = card.find_all("h6", class_="hotel-amenities-item")

    amenities_list = ";".join(
        list(map(lambda x: x.get_text(" ", strip=True), amenities))
    )

    departure_info = (
        card.find("div", class_="inline-visible")
        .find("div", class_="type")
        .get_text(" ", strip=True)
    )

    mealplan = get_text(card, "div", class_="fav-mealplan")

    room_type = get_text(card, "div", class_="fav-room")
    _, currency, price = get_text(
        card,
        "a",
        class_="price-box",
        attrs=[
            "data-currency",
            "data-price",
        ],
    )

    price_box = get_text(
        card,
        "div",
        class_="price-box-hint",
    )

    price_include = get_text(
        card,
        "ul",
        class_="price-include",
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
        "stars_class": stars_class,
    }


def handler(event, context):
    s3_client = get_s3_client()
    Bucket = event["messages"][0]["details"]["bucket_id"]
    Key = event["messages"][0]["details"]["object_id"]
    data = load_process_html_cards_from_s3(
        s3_client, Bucket, Key, get_cards, parse_card
    )
    if data is None:
        return 0

    length = len(data)

    session = initialize_session()

    table = RawTeztour("parser", session, max_records=3)
    table.bulk_upsert(data)

    return {
        "objects": length,
        "statusCode": 200,
    }
