import os
import re
import json
import boto3
import logging

import datetime as dt

from bs4 import BeautifulSoup
from collections import defaultdict
from typing import List, Any, Callable
from urllib.parse import urlparse, parse_qs


class HTMLDataExtractor:
    def __init__(self, raw_html, meta):
        self.hotel_amounts = None
        self.html = raw_html
        self.meta = meta
        self.list_of_html_attrs = ['photos', 'link', 'hotel_name', 'city', 'stars', 'items', 'price',
                                   'internal_hotel_id', 'tutu_rating']

        self.list_of_extra_attrs = ['nights_min', 'nights_max', 'date_begin', 'date_end']

        self.list_of_meta_attrs = ['website', 'airport_distance', 'beach_line', 'bucket', 'country_name',
                                   'created_dttm_utc', 'is_flight_included', 'key', 'link', 'offer_hash', 'parsing_id',
                                   'row_extracted_dttm_utc', 'row_id', 'sand_beach_flg']

        self.list_of_all_attrs = self.list_of_html_attrs + self.list_of_extra_attrs + self.list_of_meta_attrs

    def extract_all_cards(self) -> List:
        soup = BeautifulSoup(self.html, 'html.parser')
        cards = soup.find_all('div', class_='b-tours__card__hotel_wrapper')
        return cards

    def extract_raw_data_from_all_cards(self) -> defaultdict[Any, list]:
        cards = self.extract_all_cards()
        self.hotel_amounts = len(cards)

        soup_cards_dict = defaultdict(list)
        for k, v in dict.fromkeys(self.list_of_html_attrs).items():
            soup_cards_dict[k] = []

        for i, card in enumerate(cards):
            for attr in self.list_of_html_attrs:
                attr_extractor = getattr(HTMLDataExtractor, attr)
                try:
                    soup_cards_dict[attr].append(attr_extractor(card))
                except Exception as e:  # it's parsing, shit happens
                    soup_cards_dict[attr].append('None')

        return soup_cards_dict

    def extract_extra_data(self, soup_cards_dict):
        for attr in self.list_of_extra_attrs:
            attr_extractor = getattr(HTMLDataExtractor, attr)
            try:
                soup_cards_dict[attr] = attr_extractor(soup_cards_dict)
            except Exception:  # it's parsing, shit happens
                soup_cards_dict[attr] = [None for _ in soup_cards_dict.get(self.list_of_html_attrs[0])]
        return soup_cards_dict

    def extract_meta_data(self, data_dict):

        return

    def extract(self):
        data_dict = self.extract_raw_data_from_all_cards()
        data_dict = self.extract_extra_data(data_dict)
        return data_dict

    @staticmethod
    def nights_min(soup_cards_dict):
        nights_min = [parse_qs(urlparse(link).query).get('nights_min') if link is not None else None
                      for link in soup_cards_dict.get('link')]
        nights_min = [int(nights[0]) if nights is not None else None for nights in nights_min]
        return nights_min

    @staticmethod
    def nights_max(soup_cards_dict):
        nights_max = [parse_qs(urlparse(link).query).get('nights_max') if link is not None else None
                      for link in soup_cards_dict.get('link')]
        nights_max = [int(nights[0]) if nights is not None else None for nights in nights_max]
        return nights_max

    @staticmethod
    def date_begin(soup_cards_dict):
        date_begin = [parse_qs(urlparse(link).query)['date_begin'] if link is not None else None
                      for link in soup_cards_dict.get('link')]

        date_begin = [dt.datetime.strptime(date, "%d.%m.%Y").date() if date is not None else None
                      for date in date_begin]
        return date_begin

    def date_end(self, soup_cards_dict):
        date_begin = self.date_begin(soup_cards_dict)
        nights_max = self.nights_max(soup_cards_dict)
        date_end = [date_begin[i] + dt.timedelta(days=nights_max[i]) if all(
            [date_begin[i], nights_max[i]]) is not None else None
                    for i in range(date_begin)]
        return date_end

    @staticmethod
    def photos(one_card):
        photos = one_card.find('div', class_='b-tours__card__hotel j-tours_card').attrs['data-images']
        return photos

    @staticmethod
    def link(one_card):
        link = one_card.find('a', class_='card_gallery_image j-card_gallery_image j-log_card--hotel-link').attrs['href']
        link = f'https://tours.tutu.ru{link}' if link != 'None' else None
        return link

    @staticmethod
    def hotel_name(one_card):
        hotel_name = one_card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
        return hotel_name

    @staticmethod
    def city(one_card):
        city = one_card.find('div', class_='hotel_place t-txt-s t-b').text
        city = re.sub(r'\s+', ' ', city).strip() if city != 'None' else None
        return city

    @staticmethod
    def stars(one_card):
        stars = one_card.find('div', class_='hotel_rating').get('class')[1]
        stars = stars[-1] if stars != 'None' else None
        return stars

    @staticmethod
    def items(one_card):
        items = [i.text for i in one_card.find_all('div', class_='hotel_item')]
        items = [re.sub(r'\s+', ' ', i).strip() for i in items] if len(items) != 0 else None
        return items

    @staticmethod
    def price(one_card):
        price = one_card.find('span', class_='t-b').text
        price = int(re.sub(r'\D', '', price).strip()) if price != 'None' else None
        return price

    @staticmethod
    def internal_hotel_id(one_card):
        internal_hotel_id = one_card.find('div',
                                          class_='b-compare-link compare j-compare_toggle j-log__compare b-compare-link--hotel_card').attrs[
            'data-hotel_id']
        return internal_hotel_id

    @staticmethod
    def tutu_rating(one_card):
        tutu_rating = one_card.find('div', class_='card_rating_text').text
        tutu_rating = float(re.sub(r'\D', '', tutu_rating).strip()) / 10 if tutu_rating != 'None' else None
        return tutu_rating


import time

start = time.time()
html = open('./content.html', "r")
meta = open('./meta.json')
meta = json.load(meta)
extractor = HTMLDataExtractor(html, meta)
data = extractor.extract()
print(data)
print(time.time() - start)


# boto_session = boto3.session.Session(
#     aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
#     aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
# )
#
# s3 = boto_session.client(
#     service_name='s3',
#     endpoint_url='https://storage.yandexcloud.net',
#     region_name='ru-central1'
# )
#
# def load_process_html_cards_from_s3(self,
#                                     client,
#                                     Bucket: str,
#                                     Key: str,
#                                     get_cards: Callable,
#                                     parse_card: Callable) -> list:
#     prefix = "/".join(Key.split("/")[:-1])
#     object_key = os.path.join(prefix, "content.html")
#     meta_key = os.path.join(prefix, "meta.json")
#
#     meta_object_response = client.get_object(
#         Bucket=Bucket, Key=meta_key)
#
#     meta = json.loads(meta_object_response["Body"].read())
#
#     if not meta["failed"]:
#         logging.info("Start parsing object")
#         get_object_response = client.get_object(
#             Bucket=Bucket, Key=object_key)
#         content = get_object_response["Body"].read()
#         soup = BeautifulSoup(content, "html.parser")
#         cards = get_cards(soup)
#         result = list(map(parse_card, cards))
#         logging.info("End parsing object")
#         result_with_meta = update_dicts(
#             result, parsing_id=meta["parsing_id"], key=Key, bucket=Bucket)
#
#         logging.info("Deleting objects")
#         client.delete_object(Bucket=Bucket, Key=Key)
#         client.delete_object(Bucket=Bucket, Key=object_key)
#         client.delete_object(Bucket=Bucket, Key=meta_key)
#         return result_with_meta
#     else:
#         logging.error("Failed flg in meta")
#         client.delete_object(Bucket=Bucket, Key=Key)
#         client.delete_object(Bucket=Bucket, Key=meta_key)
#         return None