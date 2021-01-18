CREATE MATERIALIZED VIEW elec_aggregate.data_history_day_hotel_view TO elec_aggregate.data_history_day_hotel
AS
SELECT hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMMDD(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_day_room_view TO elec_aggregate.data_history_day_room
AS
SELECT room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMMDD(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_day_view TO elec_aggregate.data_history_day
AS
SELECT device_id,
       original_id,
       room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMMDD(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_hour_hotel_view TO elec_aggregate.data_history_hour_hotel
AS
SELECT hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toStartOfHour(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_hour_room_view TO elec_aggregate.data_history_hour_room
AS
SELECT device_id,
       original_id,
       room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toStartOfHour(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_hour_view TO elec_aggregate.data_history_hour
AS
SELECT device_id,
       original_id,
       room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toStartOfHour(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_month_hotel_view TO elec_aggregate.data_history_month_hotel
AS
SELECT hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMM(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_month_room_view TO elec_aggregate.data_history_month_room
AS
SELECT room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMM(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_month_view TO elec_aggregate.data_history_month
AS
SELECT device_id,
       original_id,
       room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYYYYMM(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_year_hotel_view TO elec_aggregate.data_history_year_hotel
AS
SELECT hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYear(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_year_room_view TO elec_aggregate.data_history_year_room
AS
SELECT room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYear(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW elec_aggregate.data_history_year_view TO elec_aggregate.data_history_year
AS
SELECT device_id,
       original_id,
       room_id,
       hotel_gid,
       int32DataMap.data_key,
       `int32DataMap.int_value`,
       int64DataMap.data_key,
       int64DataMap.int_value,
       floatDataMap.data_key,
       `floatDataMap.float_value`,
       toYear(collect_time) AS collect_time
FROM elec_aggregate.data_history;

