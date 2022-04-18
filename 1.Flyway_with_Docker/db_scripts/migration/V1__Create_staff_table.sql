CREATE TABLE IF NOT EXISTS staff
(
    id                  serial       NOT NULL,
    username            VARCHAR(50)  NOT NULL,
    product             VARCHAR(100) NOT NULL,
    system              VARCHAR(100) NOT NULL,
    email               VARCHAR(255) NOT NULL UNIQUE,

    PRIMARY KEY (id)
    );