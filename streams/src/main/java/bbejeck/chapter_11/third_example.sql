-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

-- instructions to set up ksqlCLI
-- start docker compose
-- then from terminal window docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
-- or from IntelliJ terminal  ksql http://ksqldb-server:8088

-- run this statement to create the stream setting source to use PROTOBUF
CREATE STREAM user_activity (first_name VARCHAR,
                             last_name VARCHAR,
                             activity VARCHAR,
                             event_time VARCHAR,
                             steps INT

     ) WITH (kafka_topic='user_activity',
    partitions=1,
    value_format='PROTOBUF',
    timestamp = 'event_time',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
);
-- insert statements to get started to run queries
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Art', 'Vandelay', 'walking', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP()), 'yyyy-MM-dd HH:mm:ss'), 200);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Mike', 'Rogers', 'running', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 2)), 'yyyy-MM-dd HH:mm:ss'), 10000);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Bruce', 'Banner', 'hulk-smash', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 3)), 'yyyy-MM-dd HH:mm:ss'), 300);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Art', 'Vandelay', 'cycling', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 4)), 'yyyy-MM-dd HH:mm:ss'), 200);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Mike', 'Rogers', 'running', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 5)), 'yyyy-MM-dd HH:mm:ss'), 20000);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Jane', 'Smith', 'cycling', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 6)), 'yyyy-MM-dd HH:mm:ss'), 20000);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Jane', 'Smith', 'walking', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 7)), 'yyyy-MM-dd HH:mm:ss'), 2000);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Steve', 'Jones', 'lifting', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 8)), 'yyyy-MM-dd HH:mm:ss'), 400);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('Mike', 'Rogers', 'sprinting', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP() + (60 * 1000 * 9)), 'yyyy-MM-dd HH:mm:ss'), 5000);
INSERT INTO user_activity (first_name, last_name, activity, event_time, steps ) VALUES ('John', 'Hechinger', 'walking', FORMAT_TIMESTAMP(FROM_UNIXTIME(UNIX_TIMESTAMP()  + (60 * 1000 * 10)), 'yyyy-MM-dd HH:mm:ss'), 900);

SET 'auto.offset.reset' = 'earliest';

CREATE TABLE activity_leaders WITH (value_format = 'JSON') AS
SELECT
    last_name,
    SUM(steps)
FROM user_activity
GROUP BY last_name 
EMIT CHANGES;

CREATE STREAM user_activity_avro WITH (value_format = 'AVRO') AS
  SELECT * FROM user_activity
  EMIT CHANGES;
