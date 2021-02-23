-- 设备聚合
create table IF NOT EXISTS elec_aggregate.data_history
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
        TTL collect_time  + INTERVAL 1 year
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;

create table IF NOT EXISTS elec_aggregate.data_history_hour
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
        TTL collect_time  + INTERVAL 3 year
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;


create table IF NOT EXISTS elec_aggregate.data_history_day
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
    collect_time               Date
)
    engine = SummingMergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;


-- sum
create table IF NOT EXISTS elec_aggregate.data_history_sum
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
    `floatDataMap.float_value` Array(Float32)
)
    engine = SummingMergeTree()
        PARTITION BY intHash32(room_id)
        ORDER BY (device_id, original_id, room_id, hotel_gid)
        SETTINGS index_granularity = 8192, merge_with_ttl_timeout = 900;


CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.data_history_day_view TO elec_aggregate.data_history_day
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
       toDate(collect_time) AS collect_time
FROM elec_aggregate.data_history;

CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.data_history_hour_view TO elec_aggregate.data_history_hour
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

-- sum
CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.data_history_sum_view TO elec_aggregate.data_history_sum
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
       `floatDataMap.float_value`
FROM elec_aggregate.data_history;
create table IF NOT EXISTS elec_aggregate.topic_dev_history_topic
(
    type           String,
    event_type      String,

    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,

    use_electric      Float32,
    save_electric     Float32,
    runtime         Float32,
    collect_time   UInt64
)
    engine = Kafka('127.0.0.1:9092', 'dev_history_topic', 'clickhouse', 'JSONEachRow');

create table IF NOT EXISTS elec_aggregate.topic_dev_history_topic_store
(
    type           String,
    event_type      String,

    original_id                String,
    device_id                  UInt32,
    room_id                    UInt32,
    hotel_gid                  String,

    use_electric      Float32,
    save_electric     Float32,
    runtime         Float32,
    collect_time   DateTime
) engine = MergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (device_id, original_id, room_id, hotel_gid, collect_time)
        TTL collect_time  + INTERVAL 1 year
        SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.topic_dev_history_topic_view to elec_aggregate.topic_dev_history_topic_store
as
select
    type,
    event_type,

    original_id,
    device_id,
    room_id,
    hotel_gid,

    use_electric,
    save_electric,
    runtime,
    toDateTime(collect_time/1000) as collect_time
from elec_aggregate.topic_dev_history_topic;

--030c
create table IF NOT EXISTS elec_aggregate.topic_cmd_030c_topic
(
    tId           String,
    deviceId      Int32,
    totalElectric Float32,
    use_elec      Float32,
    save_elec     Float32,
    dTime         UInt64,
    collectTime   UInt64
)
    engine = Kafka('127.0.0.1:9092', 'cmd_030c_topic', 'clickhouse', 'JSONEachRow');

create table IF NOT EXISTS elec_aggregate.topic_cmd_030c_topic_store
(
    tId           String,
    deviceId      Int32,
    totalElectric Float32,
    use_elec      Float32,
    save_elec     Float32,
    dTime         UInt64,
    collect_time   DateTime
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (tId, deviceId, collect_time)
        TTL collect_time  + INTERVAL 1 year
        SETTINGS index_granularity = 8192;

CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.topic_cmd_030c_topic_view to elec_aggregate.topic_cmd_030c_topic_store
as
select
    tId,deviceId, totalElectric, use_elec, save_elec, dTime,
    toDateTime(collectTime/1000) as collect_time
from elec_aggregate.topic_cmd_030c_topic;



create table IF NOT EXISTS elec_aggregate.ac_device
(
    id                       Int64,
    ac_name                  Nullable(String),
    room_id                  Nullable(Int32),
    hotel_gid                Nullable(String),
    t_id                     Nullable(String),
    sort_id                  Nullable(Int32),
    ac_parameter_template_id Nullable(Int32),
    create_time              DateTime,
    update_time              DateTime
)
    engine = MySQL('mysql:3306', 'nx_web', 'ac_device', 'clickhouse',
             'clickhouse');
-- account
create table IF NOT EXISTS elec_aggregate.department_architecture_room
(
    custom_gid  String,
    room_id String,
    hotel_gid String,
    update_time           DateTime
)
    engine = MySQL('mysql:3306', 'nx_web', 'department_architecture_room', 'clickhouse',
             'clickhouse');
-- 都是从kafka消费数据然进行转换
-- 开关数据转换 dev_history_topic

CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.kafka_dev_history_topic_to_data_history TO elec_aggregate.data_history
AS
SELECT original_id,
       device_id,
       room_id,
       hotel_gid,
       [
           if(length(event_type) == 0, 'use_electric', concat('use_electric', '|', event_type)),
           if(length(event_type) == 0, 'save_electric', concat('save_electric', '|', event_type)),
           if(length(event_type) == 0, 'runtime', concat('runtime', '|', event_type))] AS `floatDataMap.data_key`,
       [use_electric, save_electric, runtime]                                                                                  AS `floatDataMap.float_value`,
       collect_time                                                                                                            AS collect_time
FROM elec_aggregate.topic_dev_history_topic_store;

CREATE MATERIALIZED VIEW IF NOT EXISTS elec_aggregate.kafka_cmd_030c_topic_to_data_history TO elec_aggregate.data_history
AS
SELECT tId                                   AS original_id,
       deviceId                              AS device_id,
       if(isNull(room_id), 0, room_id)       AS room_id,
       if(isNull(hotel_gid), '0', hotel_gid) AS hotel_gid,
       [concat('use_electric', '|', '030C'), concat('save_electric', '|', '030C'), concat('runtime', '|', '030C')]     AS `floatDataMap.data_key`,
       [use_elec, save_elec, dTime/(3600000)]                 AS `floatDataMap.float_value`,
       collect_time       AS collect_time
FROM elec_aggregate.topic_cmd_030c_topic_store AS ah
         LEFT JOIN elec_aggregate.ac_device ON ah.deviceId = toInt32(id);

