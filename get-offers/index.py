import ydb
import os
import json
import datetime

import logging

logging.getLogger().setLevel(logging.INFO)


def initialize_driver():
    driver = ydb.Driver(
        endpoint=os.getenv("YDB_ENDPOINT"),
        database=os.getenv("YDB_DATABASE"),
        credentials=ydb.iam.MetadataUrlCredentials(),
    )

    try:
        driver.wait(fail_fast=True, timeout=5)
        return driver
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


query_clear_template = """DELETE FROM `users/offers`
WHERE user_id = {user_id}
"""


query_template = """$format = DateTime::Format("%d.%m.%Y");

$data = (
SELECT cast($format(start_date) as utf8) as start_date, 
    cast($format(end_date) as utf8) as end_date,
    title,
    country_name,
    num_nights,
    city_name,
    mealplan,
    room_type,
    price,
    price_change,
    link,
    num_stars,
    row_id,
    cast({user_id} as Int64) as user_id
FROM `parser/prod/offers`
WHERE {where_query}
LIMIT 100);

REPLACE INTO `users/offers`
SELECT start_date, 
    end_date,
    title,
    country_name,
    num_nights,
    city_name,
    mealplan,
    room_type,
    price,
    price_change,
    link,
    num_stars,
    row_id,
    user_id,
    row_number() over (partition by user_id order by price / num_nights ASC) as offer_number
FROM $data
"""

query_get_template = """SELECT *
FROM `users/offers`
WHERE user_id = {user_id}
    and offer_number > {offset} and offer_number <= {offset} + {number}
"""


def query_offer(params: dict, user_id: int, offset: int, number: int) -> str:
    driver = initialize_driver()

    if offset < 0 and not isinstance(offset, int):
        return []

    if offset == 0:
        logging.info("Zero offset, creating offers for user")
        session = ydb.SessionPool(driver)
        query_clear = query_clear_template.format(user_id=user_id)
        logging.info(repr(f"Executing query: {query_clear}"))
        session.retry_operation_sync(create_execute_query(query_clear))

        country = params["country_name"]
        min_nights = params["min_nights"]
        max_nights = params["max_nights"]
        num_stars = params["num_stars"]
        min_date = params["min_departure_date"]
        max_date = datetime.date.fromisoformat(min_date) + datetime.timedelta(
            params["interval_days"]
        )

        if country is None:
            query_country = "1=1"
        else:
            query_country = f"String::Strip(country_name) = '{country}'"

        query_nights = (
            f" AND num_nights >= {min_nights} AND num_nights <= {max_nights} "
        )

        query_stars = f" AND num_stars >= {num_stars}"

        query_date = f" AND start_date >= cast('{min_date}' as date) AND start_date <= cast('{max_date}' as date)"

        where_query = query_country + query_nights + query_stars + query_date
        query = query_template.format(where_query=where_query, user_id=user_id)
        logging.info(repr(f"Executing query {query}"))
        session.retry_operation_sync(create_execute_query(query))
    else:
        logging.info("Non-zero offset")

    table_session = driver.table_client.session().create()
    logging.info("Extracting data")
    query_get = query_get_template.format(user_id=user_id, offset=offset, number=number)

    results = table_session.transaction().execute(query_get, commit_tx=True)[0].rows
    return list(map(dict, results))


def handler(event, context):
    message = json.loads(event["body"])

    from_user = message["user_id"]
    params = message["params"]
    offset = message["offset"]
    number = message["number"]
    logging.info(f"Got params {json.dumps(message)}")
    results = query_offer(params, from_user, offset, number)

    return {"statusCode": 200, "body": json.dumps(results)}
