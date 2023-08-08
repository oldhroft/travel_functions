import os

from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import List

from parseteztour.utils import (
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
    table_name = "raw/tutu"


def get_cards(soup: BeautifulSoup) -> List[dict]:
    """functions to get hotel cards from soup, change it to match your case"""
    return soup.find_all('div', class_='b-tours__card__hotel_wrapper')


@parse_func_wrapper("https://tours.tutu.ru/")
def parse_card(card: BeautifulSoup) -> dict:
    """function to parse one hotel card, change it to match your case"""
    _, href = get_text(card, "a", class_="card_gallery_image j-card_gallery_image j-log_card--hotel-link", attrs=["href"])
    _, preview_img = get_text(card, "div", class_='b-tours__card__hotel j-tours_card', attrs=["data-images"])
    _, hotel_id = get_text(card, 'div', class_='b-compare-link compare j-compare_toggle j-log__compare b-compare-link--hotel_card', attrs = ['data-hotel_id'])

    location_name = card.find('div', class_='hotel_place t-txt-s t-b').text
    hotel_rating = card.find('div', class_='card_rating_text').text
    title = card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
    price = card.find('span', class_='t-b').text

    amenities_list = ";".join([i.text for i in card.find_all('div', class_='hotel_item')])
    stars_class = card.find('div', class_='hotel_rating').get('class')[1]

    latitude = None
    longitude = None
    hint_text = None
    departure_info = None
    mealplan = None
    room_type = None
    price_include = None
    till_info = None
    price_box = None
    hotel_rating_text = None
    currency = 'RU'

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


def handler(event):
    database = os.getenv("YDB_DATABASE")

    s3_client = get_s3_client()
    Bucket = 'parsing'   # event["messages"][0]["details"]["bucket_id"]
    Key = 'parsing_data/tutu/000165c1-12c6-4547-8637-9555c5fd0952/content.html?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=YCAJEbGVjXvvY-gVZXPFehw5w%2F20230808%2Fru-central1%2Fs3%2Faws4_request&X-Amz-Date=20230808T200909Z&X-Amz-Expires=360&X-Amz-Signature=866C6903362CB296409304486AEAD51B82AA080961308DEB27FD5EE2786A8136&X-Amz-SignedHeaders=host' # event["messages"][0]["details"]["object_id"]
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


if __name__ == '__main__':
    handler(0)




    # photos:str
    # link:str
    # hotel_name:str
    # city:str
    # stars:str
    # items:str
    # price:str
    # internal_hotel_id:str
    # tutu_rating:str
    # nights_min:str
    # nights_max:str
    # date_begin:str
    # date_end:str
    # offer_hash:str
    # website:str
    # airport_distance:str
    # beach_line:str
    # bucket:str
    # country_name:str
    # created_dttm_utc:str
    # is_flight_included:str
    # key:str
    # parsing_id:str
    # row_extracted_dttm_utc:str
    # row_id:str
    # sand_beach_flg:str