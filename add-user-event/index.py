import boto3
import os
import json
import datetime
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


class Users(Table):
    table_name = "users"

    fields = ["user_id", "is_bot", "first_name", "last_name", "username"]

    def add(
        self, user_id: int, is_bot: bool, first_name: str, last_name: str, username: str
    ) -> None:
        row = (
            f"({user_id},cast({is_bot} as bool),"
            f"{nvl(first_name)},"
            f"{nvl(last_name)},"
            f"{nvl(username)})"
        )
        self._insert_row(row=row)


class Events(Table):
    table_name = "events"

    fields = ["user_id", "param", "event", "created_dttm"]

    def add(self, user_id: str, param: str, event: str, dttm: str) -> None:
        row = f'({user_id},{nvl(param)},{nvl(event)},cast("{dttm}" as datetime))'
        self._insert_row(row=row)


class EventsLog(Events):
    table_name = "events_log"


def load_to_s3(data: Union[str, dict, list], Key, Bucket, is_json=False):
    boto_session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    s3 = boto_session.client(
        service_name="s3",
        endpoint_url=os.environ["AWS_ENDPOITNT_URL"],
        region_name=os.environ["AWS_REGION_NAME"],
    )

    if isinstance(data, list) or isinstance(data, dict):
        if is_json:
            data = json.dumps(data)
        else:
            raise ValueError("You should explicitly specify is_json option")
    elif isinstance(data, str):
        pass
    else:
        raise ValueError("data should be str, list or dict")
    s3.put_object(Body=data, Bucket=Bucket, Key=Key)


def handler(event, context):
    message = json.loads(event["body"])
    from_user = message["user"]

    # load_to_s3(json.dumps(message), "message.json", "parsing", is_json=True)

    if "username" not in from_user:
        from_user["username"] = None
    if "last_name" not in from_user:
        from_user["last_name"] = None

    session = initialize_session()
    users = Users("users", session)
    events = Events("users", session)
    events_log = EventsLog("users", session)

    users.add(
        user_id=from_user["id"],
        is_bot=from_user["is_bot"],
        first_name=from_user["first_name"],
        last_name=from_user["last_name"],
        username=from_user["username"],
    )

    if message["clear"]:
        events.delete_row_where("user_id", from_user["id"])

    param = message["param"] if "param" in message else None
    time_fmt = "%Y-%m-%dT%H:%M:%SZ"
    dttm = datetime.datetime.now().strftime(time_fmt)

    events.add(user_id=from_user["id"], param=param, event=message["event"], dttm=dttm)
    events_log.add(
        user_id=from_user["id"], param=param, event=message["event"], dttm=dttm
    )

    return {
        "statusCode": 200,
        "message": message,
        "body": "Hello World!",
    }
