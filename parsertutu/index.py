import os

from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List

from utils import (
    get_text,
    BaseEntry,
    parse_func_wrapper,
    RawUtf8Table,
    get_s3_client,
    initialize_session,
    load_process_html_cards_from_s3
)


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
    title: str
    amenities_list: str
    price: str
    stars_class: str


class RawTuTu(RawUtf8Table):
    """Raw table class

    To create your table, just change the name of the class,
    and the attribute table_name

    """

    entry_class = Entry
    table_name = "raw/tutu"


def get_cards(soup: BeautifulSoup) -> List[dict]:
    """functions to get hotel cards from soup, change it to match your case"""
    return soup.find_all('div', class_='b-tours__card__hotel_wrapper')


@parse_func_wrapper("https://tours.tutu.ru/")
def parse_card(card: BeautifulSoup) -> dict:
    """function to parse one hotel card, change it to match your case"""
    try:
        _, href = get_text(card, "a", class_="card_gallery_image j-card_gallery_image j-log_card--hotel-link", attrs=["href"])
        _, preview_img = get_text(card, "div", class_='b-tours__card__hotel j-tours_card', attrs=["data-images"])
        _, hotel_id = get_text(card, 'div', class_='b-compare-link compare j-compare_toggle j-log__compare b-compare-link--hotel_card', attrs=['data-hotel_id'])

        location_name = card.find('div', class_='hotel_place t-txt-s t-b').text
        hotel_rating = card.find('div', class_='card_rating_text').text
        title = card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
        price = card.find('span', class_='t-b').text

        amenities_list = ";".join([i.text for i in card.find_all('div', class_='hotel_item')])
        stars_class = card.find('div', class_='hotel_rating').get('class')[1]
    except Exception:  # it's parsing, shit happens
        href, preview_img, location_name, hotel_id, hotel_rating, title, amenities_list, price, stars_class = [None] * 9

    return {
        "href": href,
        "preview_img": preview_img,
        "location_name": location_name,
        "hotel_id": hotel_id,
        "hotel_rating": hotel_rating,
        "title": title,
        "amenities_list": amenities_list,
        "price": price,
        "stars_class": stars_class,
    }


def handler(event):
    database = os.getenv("YDB_DATABASE")
    s3_client = get_s3_client()
    Bucket = event["messages"][0]["details"]["bucket_id"]
    Key = event["messages"][0]["details"]["object_id"]
    # bucket = 'parsing'   # event["messages"][0]["details"]["bucket_id"] # event["messages"][0]["details"]["object_id"] #TODO:change that
    # key = 'parsing_data/tutu/0100bca6-b11d-4330-bcf5-dfc0091cf2ab/content.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=YCAJEbGVjXvvY-gVZXPFehw5w%2F20230810%2Fru-central1%2Fs3%2Faws4_request&X-Amz-Date=20230810T233432Z&X-Amz-Expires=7200&X-Amz-Signature=46FDC957CA26C7719A13FA1348A897A7919A48DF2860C61A8672C39AF3F7C22C&X-Amz-SignedHeaders=host'
    data = load_process_html_cards_from_s3(s3_client, Bucket, Key, get_cards, parse_card)
    if data is None:
        return 0

    data = list(map(Entry.from_dict, data))

    length = len(data)

    session = initialize_session(database)

    table = RawTuTu("parser", database, session)
    table.bulk_upsert(data)

    return {
        "objects": length,
        "statusCode": 200,
    }