CREATE TABLE `users/offers` (
    start_date utf8, 
    end_date utf8,
    title utf8,
    country_name utf8,
    num_nights double,
    city_name utf8, 
    mealplan utf8,
    room_type utf8,
    price double,
    price_change double,
    link utf8,
    num_stars double,
    row_id utf8,
    user_id Int64,
    offer_number Int64,
    PRIMARY KEY (user_id, offer_number) 
);
