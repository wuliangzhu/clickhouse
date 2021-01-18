-- 都是从kafka消费数据然进行转换
-- 030C 数据转换成data_history 格式
CREATE MATERIALIZED VIEW elec_aggregate.kafka_to_data_history TO elec_aggregate.data_history
AS
SELECT tId                                   AS original_id,
       deviceId                              AS device_id,
       if(isNull(room_id), 0, room_id)       AS room_id,
       if(isNull(hotel_gid), '0', hotel_gid) AS hotel_gid,
       ['runtime']                           AS `int64DataMap.data_key`,
       [dTime]                               AS `int64DataMap.int_value`,
       ['use_electric', 'save_electric']     AS `floatDataMap.data_key`,
       [use_elec, save_elec]                 AS `floatDataMap.float_value`,
       toDateTime(collectTime / 1000)        AS collect_time
FROM elec_aggregate.alg_history AS ah
         LEFT JOIN elec_aggregate.ac_device ON ah.deviceId = toInt32(id);
-- 开关数据转换