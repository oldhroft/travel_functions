import os

from typing import Optional, Callable, List
from bs4 import BeautifulSoup
from dataclasses import dataclass


from utils import (
    BaseEntry,
    parse_func_wrapper,
    RawUtf8Table,
    get_s3_client,
    initialize_session,
    load_process_html_cards_from_s3
)
from utils import get_text


@dataclass
class Entry(BaseEntry):
    """Declare fields of your data here
    All the data types should be string

    """

    href: str
    preview_img: str
    location_name: str
    hotel_id: str
    hotel_rating: str
    hotel_rating_text: str
    latitude: str
    longitude: str
    title: str
    hint_text: str
    amenities_list: str
    departure_info: str
    mealplan: str
    room_type: str
    currency: str
    price: str
    price_box: str
    price_include: str
    till_info: str
    stars_class: str


class RawTeztour(RawUtf8Table):
    """Raw table class

    To create your table, just change the name of the class,
    and the attribute table_name

    """

    entry_class = Entry
    table_name = "raw/teztour"


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
    database = os.getenv("YDB_DATABASE")

    s3_client = get_s3_client()
    Bucket = event["messages"][0]["details"]["bucket_id"]
    Key = event["messages"][0]["details"]["object_id"]
    data = load_process_html_cards_from_s3(
        s3_client, Bucket, Key, get_cards, parse_card
    )
    if data is None:
        return 0

    data = list(map(Entry.from_dict, data))

    length = len(data)

    session = initialize_session(database)

    table = RawTeztour("parser", database, session, max_records=3)
    table.bulk_upsert(data)

    return {
        "objects": length,
        "statusCode": 200,
    }
