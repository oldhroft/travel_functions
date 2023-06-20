import re
import defaultdict

import pandas as pd

from bs4 import BeautifulSoup


class exctarct_from_html():
    def __init__(self, html):
        self.html = html
        self.list_of_html_attrs = ['photos', 'link', 'hotel_name', 'city', 'stars', 'items', 'price',
                                   'internal_hotel_id', 'tutu_rating']

    def exctract_from_whole_html(self):
        global data
        data = defaultdict(list)
        for k, v in dict.fromkeys(self.list_of_html_attrs).items():
            data[k] = []

        soup = BeautifulSoup(self.html, 'html.parser')
        cards = soup.find_all('div', class_='b-tours__card__hotel_wrapper')
        for i, card in enumerate(cards):
            for attr in self.list_of_html_attrs:
                attr_exctractor = getattr(exctarct_from_html, attr)
                try:
                    data[attr].append(attr_exctractor(self, card))
                except Exception as e:
                    data[attr].append('None')
        return data

    def make_processing(self):
        df = pd.DataFrame(self.exctract_from_whole_html())
        df['link'] = df['link'].apply(lambda x: f'https://tours.tutu.ru{x}' if x != 'None' else None)
        df['city'] = df['city'].apply(lambda x: re.sub(r'\s+', ' ', x).strip() if x != 'None' else None)
        df['stars'] = df['stars'].apply(lambda x: x[-1] if x != 'None' else None)
        df['items'] = df['items'].apply(lambda x: [re.sub(r'\s+', ' ', i).strip() for i in x] if len(x) != 0 else None)
        df['price'] = df['price'].apply(lambda x: int(re.sub(r'\D', '', x).strip()) if x != 'None' else None)
        df['tutu_rating'] = df['tutu_rating'].apply(
            lambda x: float(re.sub(r'\D', '', x).strip()) / 10 if x != 'None' else None)
        return df

    def photos(self, one_card):
        photos = one_card.find('div', class_='b-tours__card__hotel j-tours_card').attrs['data-images']
        return photos

    def link(self, one_card):
        link = one_card.find('a', class_='card_gallery_image j-card_gallery_image j-log_card--hotel-link').attrs['href']
        return link

    def hotel_name(self, one_card):
        hotel_name = one_card.find('a', class_='g-link _inline _dark t-ttl_second j-log_card--hotel-link').text
        return hotel_name

    def city(self, one_card):
        city = one_card.find('div', class_='hotel_place t-txt-s t-b').text
        return city

    def stars(self, one_card):
        stars = one_card.find('div', class_='hotel_rating').get('class')[1]
        return stars

    def items(self, one_card):
        items = [i.text for i in one_card.find_all('div', class_='hotel_item')]
        return items

    def price(self, one_card):
        price = one_card.find('span', class_='t-b').text
        return price

    def internal_hotel_id(self, one_card):
        internal_hotel_id = one_card.find('div',
                                          class_='b-compare-link compare j-compare_toggle j-log__compare b-compare-link--hotel_card').attrs[
            'data-hotel_id']
        return internal_hotel_id

    def tutu_rating(self, one_card):
        tutu_rating = one_card.find('div', class_='card_rating_text').text
        return tutu_rating