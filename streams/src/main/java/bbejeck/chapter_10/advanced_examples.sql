-- noinspection SqlDialectInspectionForFile

-- noinspection SqlNoDataSourceInspectionForFile

-- instructions to set up ksqlCLI
-- start docker compose with file appropriate for your chip architecture
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


