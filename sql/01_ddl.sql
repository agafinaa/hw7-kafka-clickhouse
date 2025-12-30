CREATE DATABASE IF NOT EXISTS teta;

DROP TABLE IF EXISTS teta.mv_transactions;
DROP TABLE IF EXISTS teta.kafka_raw;
DROP TABLE IF EXISTS teta.transactions;

CREATE TABLE teta.kafka_raw
(
    raw String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:9092',
    kafka_topic_list = 'transactions',
    kafka_group_name = 'ch_consumer_v2',
    kafka_format = 'JSONAsString',
    kafka_num_consumers = 1;

CREATE TABLE teta.transactions
(
    state String,
    category String,
    amount Float64
)
ENGINE = MergeTree
ORDER BY (state);

CREATE MATERIALIZED VIEW teta.mv_transactions
TO teta.transactions
AS
SELECT
    JSONExtractString(raw, 'state')    AS state,
    JSONExtractString(raw, 'category') AS category,
    JSONExtractFloat(raw,  'amount')   AS amount
FROM teta.kafka_raw;
