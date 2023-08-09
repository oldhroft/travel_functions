import boto3
import os
import json
from typing import Union, Any

import ydb
import ydb.iam


def initialize_session():
    driver = ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.iam.MetadataUrlCredentials(),
    )

    try:
        driver.wait(fail_fast=True, timeout=5)
        session = ydb.SessionPool(driver)
        return session
    except TimeoutError:
        print("Connect failed to YDB")
        print("Last reported errors by discovery:")
        print(driver.discovery_debug_details())
        exit(1)


def create_execute_query(query):
    # Create the transaction and execute query.
    def _execute_query(session):
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings()
            .with_timeout(3)
            .with_operation_timeout(2),
        )

    return _execute_query


QUERY_INSERT = "REPLACE INTO `{table_path}`(" "{fields}) VALUES" "{row}"

QUERY_DELETE = "DELETE FROM `{table_path}` WHERE {column} = {value}"


def nvl(val):
    return "NULL" if val is None else json.dumps(val)


class Table:
    table_name = "table"
    fields = []

    def __init__(self, base_dir: str, session: ydb.SessionPool) -> None:
        self.session = session
        self.base_dir = base_dir
        self.path = os.path.join(self.base_dir, self.table_name)
        self.fields_str = ",".join(self.fields)

    def _insert_row(self, row: str):
        query = QUERY_INSERT.format(
            row=row, table_path=self.path, fields=self.fields_str
        )
        self.session.retry_operation_sync(create_execute_query(query))

    def delete_row_where(self, column: str, value: Any):
        query = QUERY_DELETE.format(column=column, value=value, table_path=self.path)
        self.session.retry_operation_sync(create_execute_query(query))


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


def load_to_s3(client, data: Union[str, dict, list], Key, Bucket, is_json=False):
    if isinstance(data, list) or isinstance(data, dict):
        if is_json:
            data = json.dumps(data)
        else:
            raise ValueError("You should explicitly specify is_json option")
    elif isinstance(data, str):
        pass
    else:
        raise ValueError("data should be str, list or dict")
    client.put_object(Body=data, Bucket=Bucket, Key=Key)


def load_process_meta_from_s3(
    client,
    Bucket: str,
    Key: str,
) -> dict:
    get_object_response = client.get_object(Bucket=Bucket, Key=Key)

    meta = json.loads(get_object_response["Body"].read())

    if "failed" not in meta:
        failed = "NULL"
    else:
        failed = "true" if meta["failed"] else "false"

    return {
        "exception": meta.get("exception", ""),
        "global_id": meta.get("global_id", ""),
        "stat": json.dumps(meta["stat"]),
        "func_args": json.dumps(meta["func_args"]),
        "failed": failed,
        "parsing_started": meta["parsing_started"],
        "parsing_ended": meta["parsing_ended"],
        "website": meta["website"],
        "parsing_id": meta["parsing_id"],
    }
