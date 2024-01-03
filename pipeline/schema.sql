-- This file should contain table definitions for the database.

DROP TABLE IF EXISTS sale_event;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS item_genre;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS item;
DROP TABLE IF EXISTS artist;
DROP TABLE IF EXISTS item_type;


CREATE TABLE country(
    country_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    country VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (country_id)
);

CREATE TABLE artist(
    artist_id INT GENERATED ALWAYS AS IDENTITY,
    artist_name VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (artist_id)
);

CREATE TABLE item_type(
    item_type_id INT GENERATED ALWAYS AS IDENTITY,
    item_type VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (item_type_id)
);

CREATE TABLE item(
    item_id INT GENERATED ALWAYS AS IDENTITY,
    item_type_id INT NOT NULL,
    item_name VARCHAR NOT NULL,
    artist_id INT NOT NULL,
    item_image VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (item_id),
    FOREIGN KEY (item_type_id) REFERENCES item_type(item_type_id) ON DELETE CASCADE,
    FOREIGN KEY (artist_id) REFERENCES artist(artist_id) ON DELETE CASCADE
);

CREATE TABLE genre(
    genre_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    genre VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (genre_id)
);

CREATE TABLE item_genre(
    item_genre_id INT GENERATED ALWAYS AS IDENTITY,
    item_id INT NOT NULL,
    genre_id SMALLINT NOT NULL,
    PRIMARY KEY (item_genre_id),
    FOREIGN KEY (item_id) REFERENCES item(item_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
);


CREATE TABLE sale_event(
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY,
    sale_time TIMESTAMPTZ NOT NULL,
    amount INT NOT NULL,
    item_id INT NOT NULL,
    country_id SMALLINT NOT NULL,
    PRIMARY KEY (sale_id),
    FOREIGN KEY (item_id) REFERENCES item(item_id),
    FOREIGN KEY (country_id) REFERENCES country(country_id)
);

INSERT INTO country(country) VALUES
('United State'),
('United Kingdom'),
('France'),
('Germany');

INSERT INTO artist(artist_name) VALUES
('Ariana Grande'),
('The Weeknd'),
('Nicki Minaj'),
('Beyonce');

INSERT INTO item_type(item_type) VALUES
('album'),
('track');

INSERT INTO item(item_type_id, item_name, artist_id) VALUES
('Positions', 1, 1),
('Pink Print', 1, 3),
('Sasha Fierce', 1, 4),
('Dangerous Woman', 2, 1),
('After Hours', 1, 2),
('StarBoy', 2, 2),
('StarBoy', 1, 2);

INSERT INTO genre(genre) VALUES
('pop'),
('rock'),
('rnb'),
('hip-hop'),
('rap');

INSERT INTO item_genre(item_id, genre_id) VALUES
(1,1),
(1,3),
(2,3),
(2,5),
(3,1),
(3,3),
(4,1),
(5,1),
(5,3),
(5,4),
(6,1),
(6,3),
(6,5);

INSERT INTO sale_event(sale_time,amount,item_id,country_id) VALUES
('2024-1-3, 9:42:49', 1197, 1, 2),
('2024-1-3, 10:42:49', 997, 2, 3),
('2024-1-2, 8:42:49', 197, 3, 1),
('2024-1-3, 6:42:49', 567, 4, 4),
('2024-1-3, 10:42:49', 435, 5, 3),
('2024-1-2, 4:42:49', 1345, 6, 4),
('2024-1-3, 10:42:49', 354, 1, 2),
('2024-1-2, 11:42:49', 645, 3, 4),
('2024-1-3, 10:42:49', 354, 3, 2);
