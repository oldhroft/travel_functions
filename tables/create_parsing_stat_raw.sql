DROP TABLE `parser/parsing_stat_raws`;

CREATE TABLE `parser/parsing_stat_raws` (
    parsing_started datetime,
    parsing_ended datetime,
    stat Utf8,
    website Utf8,
    parsing_id Utf8,
    global_id Utf8,
    failed Bool,
    exception Utf8,
    func_args Utf8,
    PRIMARY KEY (parsing_started, parsing_id) 
);