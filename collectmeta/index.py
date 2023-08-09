import os


from utils import (
    Table,
    get_s3_client,
    load_process_meta_from_s3,
    initialize_session,
    load_to_s3,
)


class ParsingStats(Table):
    table_name = "parsing_stat_raws"
    fields = [
        "parsing_started",
        "parsing_ended",
        "stat",
        "website",
        "parsing_id",
        "global_id",
        "failed",
        "exception",
        "func_args",
    ]

    def add(
        self,
        parsing_started: str,
        parsing_ended: str,
        stat: str,
        website: str,
        parsing_id: str,
        global_id: str,
        failed: bool,
        exception: str,
        func_args: str,
    ) -> None:
        row = f"""(
            cast('{parsing_started}' as datetime), cast('{parsing_ended}' as datetime),
            '{stat}','{website}','{parsing_id}','{global_id}',{failed},'{exception}','{func_args}'
        )"""

        self._insert_row(row)


def handler(event, context):
    s3 = get_s3_client()
    session = initialize_session()

    table = ParsingStats("parser", session)

    Bucket = event["messages"][0]["details"]["bucket_id"]
    Key = event["messages"][0]["details"]["object_id"]

    data = load_process_meta_from_s3(s3, Bucket, Key)
    table.add(**data)

    flag_key = os.path.join("/".join(Key.split("/")[:-1]), "meta.flg")
    load_to_s3(s3, "", Key=flag_key, Bucket=Bucket)

    return {
        "objects": data,
        "statusCode": 200,
    }
