CREATE DATABASE udc;

\c udc;

CREATE TABLE IF NOT EXISTS employee (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10,2)
); 