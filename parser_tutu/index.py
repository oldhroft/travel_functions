import os
import re
import json
import time
import boto3
import hashlib
import logging

import pandas as pd
import datetime as dt

from bs4 import BeautifulSoup
from collections import defaultdict
from typing import List, Any, Callable
from urllib.parse import urlparse, parse_qs


class HTMLDataExtractor:
    def __init__(self, raw_html, meta):
        self.soup_cards_dict = {}
        self.meta = meta
        self.html = raw_html
        self.hotel_amount = 0
        self.list_of_html_attrs = ['photos', 'link', 'hotel_name', 'city', 'stars', 'items', 'price',
                                   'internal_hotel_id', 'tutu_rating']

        self.list_of_extra_attrs = ['nights_min', 'nights_max', 'date_begin', 'date_end', 'offer_hash']

        self.list_of_meta_attrs = ['website', 'airport_distance', 'beach_line', 'bucket', 'country_name',
                                   'created_dttm_utc', 'is_flight_included', 'key', 'parsing_id',
                                   'row_extracted_dttm_utc', 'row_id', 'sand_beach_flg']

        self.list_of_all_attrs = self.list_of_html_attrs + self.list_of_extra_attrs + self.list_of_meta_attrs

        self.countries_dict = {197: 'Турция',
                              145: 'ОАЭ',
                              188: 'Таиланд',
                              72: 'Египет',
                              491: 'Москва'}

    def extract_all_cards(self) -> List:
        soup = BeautifulSoup(self.html, 'html.parser')
        cards = soup.find_all('div', class_='b-tours__card__hotel_wrapper')
        return cards

    def extract_raw_data_from_all_cards(self) -> defaultdict[Any, list]:
        cards = self.extract_all_cards()
        self.hotel_amount = len(cards)

        self.soup_cards_dict = defaultdict(list)
        for k, v in dict.fromkeys(self.list_of_html_attrs).items():
            self.soup_cards_dict[k] = []

        for i, card in enumerate(cards):
            for attr in self.list_of_html_attrs:
                attr_extractor = getattr(HTMLDataExtractor, attr)
                try:
                    self.soup_cards_dict[attr].append(attr_extractor(card))
                except Exception as e:  # it's parsing, shit happens
                    self.soup_cards_dict[attr].append(None)

        return self.soup_cards_dict

    def extract_extra_attrs(self):
        for attr in self.list_of_extra_attrs:
            attr_extractor = getattr(HTMLDataExtractor, attr)
            try:
                self.soup_cards_dict[attr] = attr_extractor(self)
            except Exception:  # it's parsing, shit happens
                self.soup_cards_dict[attr] = [None for _ in range(self.hotel_amount)]
        return self.soup_cards_dict


    def extract_meta_attrs(self):
        for attr in self.list_of_meta_attrs:
            attr_extractor = getattr(HTMLDataExtractor, attr)
            try:
                self.soup_cards_dict[attr] = attr_extractor(self)
            except Exception:  # it's parsing, shit happens
                self.soup_cards_dict[attr] = [None for _ in range(self.hotel_amount)]
        return self.soup_cards_dict

    def extract(self):
        self.extract_raw_data_from_all_cards()
        self.extract_extra_attrs()
        self.extract_meta_attrs()
        return self.soup_cards_dict

    # all meta attrs

    def website(self):
        single_website = self.meta['website']
        websites = [single_website for _ in range(self.hotel_amount)]
        return websites

    def airport_distance(self):
        airport_distances = [None for _ in range(self.hotel_amount)]
        return airport_distances

    def beach_line(self):
        beach_lines = [None for _ in range(self.hotel_amount)]
        return beach_lines

    def bucket(self):
        buckets = [None for _ in range(self.hotel_amount)]
        return buckets

    def country_name(self):
        single_country_name = self.meta.get('func_args').get('departure_country_id')
        country_names = [self.countries_dict.get(single_country_name) for _ in range(self.hotel_amount)]
        return country_names

    def created_dttm_utc(self):
        single_created_dttm_utc = dt.datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        created_dttms_utc = [single_created_dttm_utc for _ in range(self.hotel_amount)]
        return created_dttms_utc

    def is_flight_included(self):
        is_flights_included = [True for _ in range(self.hotel_amount)]
        return is_flights_included

    def key(self):
        keys = [None for _ in range(self.hotel_amount)]
        return keys

    def parsing_id(self):
        parsing_ids = [self.meta.get('parsing_id') for _ in range(self.hotel_amount)]
        return parsing_ids

    def row_extracted_dttm_utc(self):
        row_extracted_dttms_utc = [self.meta.get('parsing_ended') for _ in range(self.hotel_amount)]
        return row_extracted_dttms_utc

    def row_id(self):
        row_ids = ['?' for _ in range(self.hotel_amount)]
        return row_ids

    def sand_beach_flg(self):
        sand_beach_flgs = [None for _ in range(self.hotel_amount)]
        return sand_beach_flgs

    # all extra attrs
    def nights_min(self):
        nights_min = [parse_qs(urlparse(link).query).get('nights_min') if link is not None else None
                      for link in self.soup_cards_dict.get('link')]
        nights_min = [int(nights[0]) if nights is not None else None for nights in nights_min]
        return nights_min

    def nights_max(self):
        nights_max = [parse_qs(urlparse(link).query).get('nights_max') if link is not None else None
                      for link in self.soup_cards_dict.get('link')]
        nights_max = [int(nights[0]) if nights is not None else None for nights in nights_max]
        return nights_max

    def date_begin(self):

        date_begin = [parse_qs(urlparse(link).query).get('date_begin') if link is not None else None
                      for link in self.soup_cards_dict.get('link')]
        date_begin = [i[0] if i is not None else None for i in date_begin]

        date_begin = [str(dt.datetime.strptime(date, "%d.%m.%Y").date()) if date is not None else None
                      for date in date_begin]
        return date_begin

    def date_end(self):
        date_begin = self.soup_cards_dict.get('date_begin')
        nights_max = self.soup_cards_dict.get('nights_max')

        date_end = [dt.datetime.strptime(date_begin[i], '%Y-%m-%d') + dt.timedelta(days=nights_max[i])
                    if all([date_begin[i], nights_max[i]]) else None
                    for i in range(len(date_begin))]
        return date_end

    def offer_hash(self):
        hotel_id = self.soup_cards_dict.get('internal_hotel_id')
        to_hash = [f'{i}{time.time_ns()}' for i in hotel_id]
        to_hash = [hashlib.sha256(i.encode("utf-8")).hexdigest() for i in to_hash]
        return to_hash

    # all basic html_attrs
    @staticmethod
    def photos(one_card):
        photos = one_card.find('div', class_='b-tours__card__hotel j-tours_card').attrs['data-images']
        return photos

    @staticmethod
    def link(one_card):
        link = one_card.find('a', class_='card_gallery_image j-card_gallery_image j-log_card--hotel-link').attrs['href']
        link = f'https://tours.tutu.ru{link}' if link is not None else None
        return link

    @staticmethod
    def hotel_name(one_card):
        hotel_name = one_card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
        return hotel_name

    @staticmethod
    def city(one_card):
        city = one_card.find('div', class_='hotel_place t-txt-s t-b').text
        city = re.sub(r'\s+', ' ', city).strip() if city is not None else None
        return city

    @staticmethod
    def stars(one_card):
        stars = one_card.find('div', class_='hotel_rating').get('class')[1]
        stars = stars[-1] if stars is not None else None
        return stars

    @staticmethod
    def items(one_card):
        items = [i.text for i in one_card.find_all('div', class_='hotel_item')]
        items = [re.sub(r'\s+', ' ', i).strip() for i in items] if len(items) != 0 else None
        return items

    @staticmethod
    def price(one_card):
        price = one_card.find('span', class_='t-b').text
        price = int(re.sub(r'\D', '', price).strip()) if price is not None else None
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
        tutu_rating = float(re.sub(r'\D', '', tutu_rating).strip()) / 10 if tutu_rating is not None else None
        return tutu_rating


start = time.time()
html = open('./content.html', "r")
meta = json.load(open('./meta.json'))
data = HTMLDataExtractor(html, meta).extract()
print(data)
print(time.time() - start)