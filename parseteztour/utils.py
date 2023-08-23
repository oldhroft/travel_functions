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


def initialize_session(database):
    driver_config = ydb.DriverConfig(
        os.environ["YDB_ENDPOINT"],
        os.environ["YDB_DATABASE"],
        credentials=ydb.iam.MetadataUrlCredentials()
    )
    driver = ydb.Driver(driver_config)

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
            result["created_dttm"] = int(
                time.mktime(datetime.datetime.now().timetuple())
            )
            result["website"] = website
            result["link"] = urljoin(website, result["href"])
            result["offer_hash"] = md5(result["link"].encode()).hexdigest()
            result["row_id"] = str(uuid.uuid4())
            return result

        return somedec_inner

    return dec_outer


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


from dataclasses import dataclass, fields as get_fields


@dataclass
class BaseEntry:
    created_dttm: int
    website: str
    link: str
    offer_hash: str
    row_id: str
    parsing_id: str
    key: str
    bucket: str

    @classmethod
    def from_dict(cls, record: dict):
        return cls(**record)


class RawUtf8Table:
    entry_class = BaseEntry
    table_name = "raw/table"

    def __init__(
        self,
        base_dir: str,
        database: str,
        client: ydb.TableClient,
    ) -> None:
        self.client = client
        self.base_dir = base_dir
        self.path = os.path.join(database, base_dir, self.table_name)

        column_types = ydb.BulkUpsertColumns()

        self.fields = [field.name for field in get_fields(self.entry_class)]
        for field in self.fields:
            if field == "created_dttm":
                field_type = ydb.PrimitiveType.Datetime
            else:
                field_type = ydb.PrimitiveType.Utf8

            if field not in ["created_dttm", "parsing_id", "row_id"]:
                field_type = ydb.OptionalType(field_type)

            column_types.add_column(field, field_type)

        self.column_types = column_types

    def bulk_upsert(self, data: List):
        self.client.bulk_upsert(self.path, data, self.column_types)


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
