import re

import pandas as pd

from typing import List, Any
from bs4 import BeautifulSoup
from collections import defaultdict
from urllib.parse import urlparse, parse_qs


class HTMLDataExtractor:
    def __init__(self, raw_html):
        self.html = raw_html
        self.list_of_html_attrs = ['photos', 'link', 'hotel_name', 'city', 'stars', 'items', 'price',
                                   'internal_hotel_id', 'tutu_rating']

    def extract_all_cards(self) -> List:
        soup = BeautifulSoup(self.html, 'html.parser')
        cards = soup.find_all('div', class_='b-tours__card__hotel_wrapper')
        return cards

    def extract_raw_data_from_all_cards(self) -> defaultdict[Any, list]:
        cards = self.extract_all_cards()

        data_dict = defaultdict(list)
        for k, v in dict.fromkeys(self.list_of_html_attrs).items():
            data_dict[k] = []

        for i, card in enumerate(cards):
            for attr in self.list_of_html_attrs:
                attr_extractor = getattr(HTMLDataExtractor, attr)
                try:
                    data_dict[attr].append(attr_extractor(card))
                except Exception:  # it's parsing, shit happens
                    data_dict[attr].append('None')
        return data_dict

    def make_pandas_processing(self) -> pd.DataFrame:  # useless because we decided not to use pandas
        df = pd.DataFrame(self.extract_raw_data_from_all_cards())
        df['link'] = df['link'].apply(lambda x: f'https://tours.tutu.ru{x}' if x != 'None' else None)
        df['city'] = df['city'].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if x != 'None' else None)
        df['stars'] = df['stars'].apply(lambda x: x[-1] if x != 'None' else None)
        df['items'] = df['items'].apply(lambda x: [re.sub(r'\s+', ' ', i).strip() for i in x] if len(x) != 0 else None)
        df['price'] = df['price'].apply(lambda x: int(re.sub(r'\D', '', x).strip()) if x != 'None' else None)
        df['tutu_rating'] = df['tutu_rating'].apply(
            lambda x: float(re.sub(r'\D', '', x).strip()) / 10 if x != 'None' else None)

        df['nights_min'] = df['link'].apply(
            lambda x: int(parse_qs(urlparse(x).query)['nights_min'][0]) if x is not None else None)
        df['nights_max'] = df['link'].apply(
            lambda x: int(parse_qs(urlparse(x).query)['nights_max'][0]) if x is not None else None)
        df['date_begin'] = df['link'].apply(lambda x: pd.to_datetime(parse_qs(urlparse(x).query)['date_begin'][0],
                                                                     infer_datetime_format=True).date() if x is not None else None)
        df['date_end'] = df.apply(lambda x: x.date_begin + pd.Timedelta(value=f'{x.nights_max}day') if all(
            [x.nights_min, x.date_begin]) else None, axis=1)

        return df

    @staticmethod
    def photos(one_card):
        photos = one_card.find('div', class_='b-tours__card__hotel j-tours_card').attrs['data-images']
        return photos

    @staticmethod
    def link(one_card):
        link = one_card.find('a', class_='card_gallery_image j-card_gallery_image j-log_card--hotel-link').attrs['href']
        return link

    @staticmethod
    def hotel_name(one_card):
        hotel_name = one_card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
        return hotel_name

    @staticmethod
    def city(one_card):
        city = one_card.find('div', class_='hotel_place t-txt-s t-b').text
        return city

    @staticmethod
    def stars(one_card):
        stars = one_card.find('div', class_='hotel_rating').get('class')[1]
        return stars

    @staticmethod
    def items(one_card):
        items = [i.text for i in one_card.find_all('div', class_='hotel_item')]
        return items

    @staticmethod
    def price(one_card):
        price = one_card.find('span', class_='t-b').text
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
        return tutu_rating


# html = open('./content.html', "r")
# extractor = HTMLDataExtractor(html)
# data = extractor.make_pandas_processing()
# print(data)

# import os
# import ydb
# import json
# import uuid
# import boto3
# import logging
# import datetime
#
# from hashlib import md5
# from functools import wraps
# from bs4 import BeautifulSoup
# from urllib.parse import urljoin
# from typing import Optional, Callable, List
