-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

-- instructions to set up ksqlCLI
-- start docker compose
-- then from terminal window docker exec -it ksqldb_cli ksql http://ksqldb_server:8088
-- or from IntelliJ terminal  ksql http://ksqldb_server:8088

-- run this statement to create the stream setting source to use PROTOBUF
-- start join example
CREATE STREAM coffee_purchase_stream (custId VARCHAR KEY,
                                      drink VARCHAR,
                                      drinkSize VARCHAR,
                                      price DOUBLE,
                                      purchaseDate BIGINT)
     WITH (kafka_topic = 'coffee_purchase',
           partitions = 1,
           value_format = 'PROTOBUF',
           timestamp = 'purchaseDate'
     );


CREATE STREAM store_purchase_stream(custId VARCHAR KEY,
                                    credit_card VARCHAR,
                                    purchaseDate BIGINT,
                                    storeId VARCHAR,
                                    total DOUBLE)
     WITH (kafka_topic = 'store_purchase',
           partitions = 1,
           value_format = 'PROTOBUF',
           timestamp = 'purchaseDate'
     );
-- insert statements to get started to run queries
INSERT INTO coffee_purchase_stream (custID, drink, drinkSize, price, purchaseDate) VALUES ('12345', 'mocha', 'small', 4.99, UNIX_TIMESTAMP());
INSERT INTO coffee_purchase_stream (custID, drink, drinkSize, price, purchaseDate) VALUES ('54321', 'drip_coffee', 'large', 2.99, UNIX_TIMESTAMP() + (60 * 1000 * 2));
INSERT INTO coffee_purchase_stream (custID, drink, drinkSize, price, purchaseDate) VALUES ('98765', 'ice_tea', 'large', 5.99, UNIX_TIMESTAMP() + (60 * 1000 * 3));
INSERT INTO coffee_purchase_stream (custID, drink, drinkSize, price, purchaseDate) VALUES ('45678', 'mocha', 'large', 7.99, UNIX_TIMESTAMP() + (60 * 1000 * 4));
INSERT INTO coffee_purchase_stream (custID, drink, drinkSize, price, purchaseDate) VALUES ('34567', 'americano', 'large', 6.99, UNIX_TIMESTAMP() + (60 * 1000 * 5));

INSERT INTO store_purchase_stream (custId, credit_card, purchaseDate, storeId, total) VALUES ('12345', '123456789', UNIX_TIMESTAMP() + (60 * 1000 * 16), 'A1234', 19.89);
INSERT INTO store_purchase_stream (custId, credit_card, purchaseDate, storeId, total) VALUES ('54321', '234567890', UNIX_TIMESTAMP() + (60 * 1000 * 17), 'A1234', 50.24);
INSERT INTO store_purchase_stream (custId, credit_card, purchaseDate, storeId, total) VALUES ('98765', '345678901', UNIX_TIMESTAMP() + (60 * 1000 * 18), 'A1234', 5.99);
INSERT INTO store_purchase_stream (custId, credit_card, purchaseDate, storeId, total) VALUES ('45678', '456789012', UNIX_TIMESTAMP() + (60 * 1000 * 19), 'A1234', 30.37);
INSERT INTO store_purchase_stream (custId, credit_card, purchaseDate, storeId, total) VALUES ('34567', '567890123', UNIX_TIMESTAMP()  + (60 * 1000 * 38), 'A1234', 104.79);

SET 'auto.offset.reset' = 'earliest';

CREATE STREAM customer_rewards_stream AS
SELECT c.custId AS customerId,
    s.total as amount,
    CASE
    WHEN s.total < 25.00 THEN 15
    WHEN s.total < 50.00 THEN 50
    ELSE 75
END AS reward_points
  FROM coffee_purchase_stream c
    INNER JOIN store_purchase_stream s
    WITHIN 30 MINUTES GRACE PERIOD 2 MINUTES
    ON c.custId = s.custId
EMIT CHANGES;
--end join example

-- start enriched join section

CREATE TABLE rewards_members (member_id VARCHAR PRIMARY KEY,
                              first_name VARCHAR,
                              last_name VARCHAR,
                              address VARCHAR,
                              year_joined INT)
    WITH (kafka_topic = 'rewards_members',
        partitions = 1,
        value_format = 'PROTOBUF'
        );

INSERT INTO rewards_members (member_id, first_name, last_name, address, year_joined) VALUES ('12345', 'John', 'Hechinger', '123 University Blvd', 1980);
INSERT INTO rewards_members (member_id, first_name, last_name, address, year_joined) VALUES ('54321', 'Art', 'Vandelay', '406 56th Street', 1995);
INSERT INTO rewards_members (member_id, first_name, last_name, address, year_joined) VALUES ('98765', 'Bob', 'Sacamano', '25 Park Ave West', 1996);
INSERT INTO rewards_members (member_id, first_name, last_name, address, year_joined) VALUES ('45678', 'Beth', 'Judge', '24 Amber Tree Way', 1999);

CREATE STREAM enriched-rewards-stream
   WITH (kafka_topic='customer_rewards_stream',
          value_format='PROTOBUF') AS
SELECT crs.custID as customer_id,
    rm.first_name + ' ' + rm.last_name as name,
    rm.year_joined as member_since
    crs.amount as total_purchase,
    crs.reward_points as points
FROM customer_rewards_stream crs
    LEFT OUTER JOIN rewards_members rm
on crs.customerId = rm.member_id

-- JSON nested structures

CREATE STREAM school_event_stream (
  event_id INT,
  event STRUCT<type VARCHAR,
               date VARCHAR,
               student STRUCT<first_name VARCHAR,
                              last_name VARCHAR,
                              id BIGINT,
                              email VARCHAR
                              >,
                class STRUCT<name VARCHAR,
                             room VARCHAR,
                             professor STRUCT<first_name VARCHAR,
                                              last_name VARCHAR,
                                              other_classes ARRAY<VARCHAR>
                                             >
                            >
                >
)
WITH (kafka_topic='school_events',
     partitions=1,
     value_format='JSON'
)

SET 'auto.offset.reset' = 'earliest';

"docker exec -it broker /usr/bin/kafka-console-producer --bootstrap-server broker:9092 --topic school_events"

{ "event_id": "1", "event": { "type": "registration", "date": "2023-11-18", "student": {"first_name": "Rocky", "last_name":"Squirrel", "id":12345 , "email": "rsquirrel@gmail.com"}, "class": { "name": "Principals of Flight", "room": "GH0341", "professor": {"first_name": "Boris", "last_name":"Badenov", "other_classes": ["Spying 101", "Surveillance ", "History"] } } } }
{ "event_id": "2", "event": { "type": "registration", "date": "2023-11-18", "student": {"first_name": "Fred", "last_name":"Flintsone", "id":33456 , "email": "fflintstone@gmail.com"}, "class": { "name": "Geology", "room": "RQ2331", "professor": {"first_name": "Bullwinkle", "last_name": "Moose", "other_classes": ["Frostbite Falls History", "Antler Care"] } } } }
{ "event_id": "3", "event": { "type": "registration", "date": "2023-11-18", "student": {"first_name": "George", "last_name":"Jetson", "id":44557 , "email": "gjetson@gmail.com"}, "class": { "name": "Sprocket Design", "room": "ENG1123", "professor": {"first_name": "Cosmo", "last_name": "Spacely", "other_classes": ["Sprockets 201", "Advanced Sprocket Design"] } } } }
{ "event_id": "4", "event": { "type": "registration", "date": "2023-11-18", "student": {"first_name": "Johnny", "last_name":"Quest", "id":72064 , "email": "questguy@gmail.com"}, "class": { "name": "Advanced Navigation", "room": "JG8876", "professor": {"first_name": "Race", "last_name": "Bannon", "other_classes": ["Self Defense", "Flying Jets", "Hand to Hand Combat"] } } } }

SELECT
   event->student->id as student_id,
   event->student->email as student_email,
   event->class->professor->other_classes as suggested
FROM
   school_event_stream
EMIT CHANGES;

SELECT
    event->student->first_name as student_name,
    event->student->email as student_email,
    event->class->professor->other_classes[1] as suggested
FROM
   school_event_stream
EMIT CHANGES;