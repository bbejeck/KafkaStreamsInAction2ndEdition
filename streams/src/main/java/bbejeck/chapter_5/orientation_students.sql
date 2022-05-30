DROP TABLE IF EXISTS orientation_students;

CREATE TABLE orientation_students
(
    ssn        VARCHAR(11)  NOT NULL,
    email      VARCHAR(255) NOT NULL,
    user_name  VARCHAR(255) NOT NULL PRIMARY KEY ,
    full_name  VARCHAR(255) NOT NULL,
    address    VARCHAR(255) NOT NULL,
    state      VARCHAR(255) NOT NULL,
    major      VARCHAR(255) NOT NULL,
    overnight  CHARACTER DEFAULT 'N',
    ts         TIMESTAMP DEFAULT NOW() NOT NULL 
);


CREATE OR REPLACE FUNCTION update_ts_column_on_update()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.ts = now();
    RETURN NEW;
END;
$$ language 'plpgsql';


CREATE TRIGGER update_orientation_students_timestamp BEFORE UPDATE
    ON orientation_students FOR EACH ROW EXECUTE PROCEDURE
    update_ts_column_on_update();

INSERT INTO orientation_students VALUES ('123-02-9865', 'vandelay@gmail.com', 'artv', 'Art Vandelay', '123 Beach Drive, Hamptons', 'NY', 'Architecture', 'Y');
INSERT INTO orientation_students VALUES ('331-02-2865', 'brubble@hotmail.com', 'bettyrub', 'Betty Rubble', '405 Elm Street, Rockville', 'MD', 'Finance', 'N');
INSERT INTO orientation_students VALUES ('435-12-3865', 'sstrange@marvel.com', 'timemaster', 'Stephen Strange', '177 A Bleeker Street, New York CIty', 'NY', 'Mystic Arts', 'Y');





