create table if not exists elec_aggregate.data_dwd
(
    original_id    String            default 0,
    room_id   UInt32             default 0,
    hotel_gid String            default 0,
    record_num UInt32 default 1,
    comfort_flag     UInt32 default 0,
    collect_time DateTime
)
    engine = MergeTree()
        PARTITION BY toYYYYMM(collect_time)
        ORDER BY (hotel_gid, room_id, original_id, collect_time)
        TTL collect_time  + INTERVAL 3 year
        SETTINGS index_granularity = 8192;

create table if not exists elec_aggregate.data_dws_day
(
	original_id    String            default 0,
    room_id   UInt32             default 0,
    hotel_gid String            default 0,
    record_num AggregateFunction(sum, UInt32),
    comfort_flag     AggregateFunction(sum, UInt32),
    collect_time Date
)
engine = AggregatingMergeTree()
PARTITION BY toYYYYMM(collect_time)
        ORDER BY (hotel_gid, room_id, original_id, collect_time)
SETTINGS index_granularity = 8192;

create table if not exists elec_aggregate.data_dws_sum
(
	original_id    String            default 0,
    room_id   UInt32             default 0,
    hotel_gid String            default 0,
    record_num AggregateFunction(sum, UInt32),
    comfort_flag     AggregateFunction(sum, UInt32)
)
engine = AggregatingMergeTree()
        ORDER BY (hotel_gid, room_id, original_id)
SETTINGS index_granularity = 8192;

create materialized view elec_aggregate.data_dws_day_view to elec_aggregate.data_dws_day as
select original_id, room_id, hotel_gid,
       sumState(record_num) as record_num,
       sumState(comfort_flag) as comfort_flag,
        toDate(collect_time) AS collect_time
from elec_aggregate.data_dwd
group by hotel_gid, room_id, original_id, collect_time;

create materialized view elec_aggregate.data_dws_sum_view to elec_aggregate.data_dws_sum as
select original_id, room_id, hotel_gid,
       sumState(record_num) as record_num,
       sumState(comfort_flag) as comfort_flag
from elec_aggregate.data_dwd
group by hotel_gid, room_id, original_id;