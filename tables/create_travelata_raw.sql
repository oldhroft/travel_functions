CREATE TABLE `parser/raw/travelata` (
    title Utf8,
    href Utf8,
    location Utf8,
    distances Utf8,
    rating Utf8,
    reviews Utf8,
    less_places Utf8,
    num_stars Utf8,
    orders_count Utf8,
    criteria Utf8,
    price Utf8,
    oil_tax Utf8,
    attributes Utf8,
    created_dttm Datetime,
    website Utf8,
    link Utf8,
    offer_hash Utf8,
    row_id Utf8,
    parsing_id Utf8,
    key Utf8,
    bucket Utf8,
    PRIMARY KEY (created_dttm, parsing_id, row_id) 
) WITH (
    TTL = Interval("PT120H") ON created_dttm
);