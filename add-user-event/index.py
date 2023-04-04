import boto3
import os
import json
from typing import Union

import ydb
import ydb.iam


def initialize_session():
    driver = ydb.Driver(
        endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'),
        credentials=ydb.iam.MetadataUrlCredentials(),)
    
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
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return _execute_query

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

    from_user =  message["message"]["from"]
    
    load_to_s3(json.dumps(message), "message.json", "parsing", is_json=True)

    if "username" not in from_user:
        from_user["username"] = None
    if "last_name" not in from_user:
        from_user["last_name"] = None

    session = initialize_session()
    table_path = "users/users"

    def nvl(val):
        return "NULL" if val is None else f'"{val}"'

    row = f'({from_user["id"]},cast({from_user["is_bot"]} as bool),'\
        f'{nvl(from_user["first_name"])},'\
        f'{nvl(from_user["last_name"])},'\
        f'{nvl(from_user["username"])})'
    
    query = f"REPLACE INTO `{table_path}`("\
        "user_id, is_bot, first_name, last_name, username) VALUES" \
        f"{row}"

    session.retry_operation_sync(create_execute_query(query))
    
    return {
        'statusCode': 200,
        "message": message,
        'body': 'Hello World!',
    }