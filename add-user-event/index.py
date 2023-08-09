import boto3
import os
import json
import datetime
from typing import Union, Any

import ydb
import ydb.iam

from utils import initialize_session, nvl, Table


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
    time_fmt = "%Y-%m-%dT%H:%M:%SZ"
    

    def add(self, user_id: str, param: str, event: str) -> None:
        dttm = datetime.datetime.now().strftime(self.time_fmt)
        row = f'({user_id},{nvl(param)},{nvl(event)},cast("{dttm}" as datetime))'
        self._insert_row(row=row)


class EventsLog(Events):
    table_name = "events_log"


def handler(event, context):
    message = json.loads(event["body"])
    from_user = message["user"]

    # load_to_s3(json.dumps(message), "message.json", "parsing", is_json=True)

    session = initialize_session()
    users = Users("users", session)
    events = Events("users", session)
    events_log = EventsLog("users", session)

    users.add(
        user_id=from_user["id"],
        is_bot=from_user["is_bot"],
        first_name=from_user["first_name"],
        last_name=from_user.get("last_name", None),
        username=from_user.get("username", None)
    )

    if message["clear"]:
        events.delete_row_where("user_id", from_user["id"])

    param = message.get("param", None)
    events.add(user_id=from_user["id"], param=param, event=message["event"])
    events_log.add(
        user_id=from_user["id"], param=param, event=message["event"]
    )

    return {
        "statusCode": 200,
        "message": message,
        "body": "Hello World!",
    }
