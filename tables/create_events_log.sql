CREATE TABLE `users/events_log` (
    user_id int,
    param utf8,
    event utf8,
    created_dttm datetime,
    PRIMARY KEY (user_id, created_dttm) 
);