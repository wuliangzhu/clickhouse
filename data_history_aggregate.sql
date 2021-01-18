create table elec_aggregate.data_history
(
    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               DateTime
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (device_id, collect_time)
        SETTINGS index_granularity = 8192;

create table elec_aggregate.data_history_day
(
    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_day_hotel
(
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_day_room
(
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_hour
(
    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               DateTime
)
    engine = SummingMergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_hour_hotel
(
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               DateTime
)
    engine = SummingMergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_hour_room
(
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               DateTime
)
    engine = SummingMergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_month
(
    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192;

create table elec_aggregate.data_history_month_hotel
(
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_month_room
(
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt32
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_year
(
    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt16
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192;

create table elec_aggregate.data_history_year_hotel
(
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt16
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table elec_aggregate.data_history_year_room
(
    room_id                    UInt32,
    hotel_gid                  String,
    `int32DataMap.data_key`    Array(String),
    `int32DataMap.int_value`   Array(UInt32),
    `int64DataMap.data_key`    Array(String),
    `int64DataMap.int_value`   Array(UInt64),
    `floatDataMap.data_key`    Array(String),
    `floatDataMap.float_value` Array(Float32),
    collect_time               UInt16
)
    engine = SummingMergeTree()
        PARTITION BY collect_time
        ORDER BY (room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;


