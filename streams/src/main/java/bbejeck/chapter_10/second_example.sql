-- noinspection SqlNoDataSourceInspectionForFile

from terminal window docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
from IntelliJ terminal  ksql http://ksqldb-server:8088

CREATE STREAM user_activity (first_name VARCHAR,
                             last_name VARCHAR,
                             activity VARCHAR,
                             event_time VARCHAR,
                             steps INT

     ) WITH (kafka_topic='user_activity',
    partitions=4,
    value_format='JSON',
    timestamp = 'event_time',
    timestamp_format = 'yyyy-MM-dd HH:mm:ss'
);

CREATE TABLE activity_leaders AS
SELECT
    last_name,
    SUM(steps)
FROM user_activity
GROUP BY last_name 
         EMIT CHANGES;

CREATE TABLE activity_leaders WITH (KEY_FORMAT = 'JSON') AS
SELECT
    first_name as key_1,
    last_name as key_2,
    activity as key_3,
    AS_VALUE(first_name) as first_name,
    AS_VALUE(last_name) as last_name,
    AS_VALUE(activity) as activity,
    SUM(steps) as total_steps
    FROM user_activity
    GROUP BY first_name, last_name, activity
    EMIT CHANGES;