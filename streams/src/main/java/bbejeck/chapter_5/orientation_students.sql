DROP TABLE IF EXISTS orientation_students;

CREATE TABLE orientation_students
(
    id         SERIAL PRIMARY KEY,
    ssn        VARCHAR(11)  NOT NULL,
    name       VARCHAR(255) NOT NULL,
    address    VARCHAR(255) NOT NULL,
    state      VARCHAR(255) NOT NULL,
    major      VARCHAR(255) NOT NULL,
    overnight  CHARACTER DEFAULT 'N',
    special    TEXT
);




