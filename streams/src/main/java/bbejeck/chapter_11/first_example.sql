-- noinspection SqlNoDataSourceInspectionForFile
-- from terminal window docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
-- from IntelliJ terminal  ksql http://ksqldb-server:8088

CREATE STREAM input_stream (phrase VARCHAR) WITH (kafka_topic='src-topic', partitions=1, value_format='KAFKA');

CREATE STREAM yelling AS
SELECT UCASE(phrase) AS SHOUT
FROM input_stream
    EMIT CHANGES;

SET 'auto.offset.reset' = 'earliest';

INSERT INTO input_stream (phrase) VALUES ('Chuck Norris finished World of Warcraft');
INSERT INTO input_stream (phrase) VALUES ('Chuck Norris first program was kill -9');
INSERT INTO input_stream (phrase) VALUES ('generate bricks-and-clicks content');
INSERT INTO input_stream (phrase) VALUES ('brand best-of-breed infomediaries');


SELECT * FROM yelling;

