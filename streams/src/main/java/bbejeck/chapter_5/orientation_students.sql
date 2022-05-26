DROP TABLE IF EXISTS orientation_students;

CREATE TABLE orientation_students
(
    id         SERIAL PRIMARY KEY,
    ssn        VARCHAR(11)  NOT NULL,
    stu_name   VARCHAR(255) NOT NULL,
    address    VARCHAR(255) NOT NULL,
    state      VARCHAR(255) NOT NULL,
    major      VARCHAR(255) NOT NULL,
    overnight  CHARACTER DEFAULT 'N',
    ts         timestamp DEFAULT NOW()
);

INSERT INTO orientation_students VALUES (DEFAULT, '123-02-9865', 'Art Vandelay', '123 Beach Drive, Hamptons', 'NY', 'Architecture', 'Y');
INSERT INTO orientation_students VALUES (DEFAULT, '331-02-2865', 'Betty Rubble', '405 Elm Street, Rockville', 'MD', 'Finance', 'N');
INSERT INTO orientation_students VALUES (DEFAULT, '435-12-3865', 'Stephen Strange', '177 A Bleeker Street, New York CIty', 'NY', 'Mystic Arts', 'Y');





