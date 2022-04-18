CREATE TABLE IF NOT EXISTS cars
(
    id              serial       NOT NULL ,
    license_plate   VARCHAR(100) NOT NULL UNIQUE,
    color           VARCHAR(30)  NOT NULL,

    PRIMARY KEY (id)
);

INSERT INTO cars (license_plate, color) VALUES ('Р111КК77', 'blue');