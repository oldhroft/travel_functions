import boto3
import ydb
import os
import json

boto_session = boto3.session.Session(
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"]
)

s3 = boto_session.client(
    service_name='s3',
    endpoint_url='https://storage.yandexcloud.net',
    region_name='ru-central1'
)

# Create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)

from typing import Union
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

def load_process_meta_from_s3(
    client, Bucket: str, Key: str,
) -> list:
    get_object_response = client.get_object(
        Bucket=Bucket, Key=Key)
    
    meta = json.loads(get_object_response["Body"].read())

    if "failed" not in meta:
        meta["failed"] = "NULL"
    else:
        meta["failed"] = "true" if meta["failed"] else "false"
    if "exception" not in meta:
        meta["exception"] = ""
    if "global_id" not in meta:
        meta["global_id"] = ""

    meta["stat"] = json.dumps(meta["stat"])
    meta["func_args"] = json.dumps(meta["func_args"])

    return meta

def format_record(record: list):
    return '''(
        cast('{parsing_started}' as datetime), cast('{parsing_ended}' as datetime),
        '{stat}','{website}','{parsing_id}','{global_id}',{failed},'{exception}','{func_args}'
    )'''.format(**record)

def create_statement(data: dict) -> str:
    query = """
    REPLACE INTO `parser/parsing_stat_raws`(
        parsing_started,
        parsing_ended,
        stat,
        website,
        parsing_id,
        global_id,
        failed,
        exception,
        func_args)
    VALUES
    {}
    """.format(format_record(data))

    return query


def create_execute_query(query):
  # Create the transaction and execute query.
    def _execute_query(session):
        session.transaction().execute(
            query,
            commit_tx=True,
            settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)
        )
    return _execute_query

def process_file(Bucket, Key):
    result = load_process_meta_from_s3(
        s3, Bucket, Key, 
    )

    query = create_statement(result)
    try:
        pool.retry_operation_sync(create_execute_query(query))
    except:
        raise ValueError(f"Failed at query {query}")
    
    flag_key = os.path.join("/".join(Key.split("/")[:-1]), "meta.flg")

    load_to_s3("", Key=flag_key, Bucket="parsing")
        
    return result
    
def handler(event, context):

    result = process_file(
        Bucket=event["messages"][0]["details"]["bucket_id"],
        Key=event["messages"][0]["details"]["object_id"]
    )

    return {
        "objects": result,
        'statusCode': 200,
    }