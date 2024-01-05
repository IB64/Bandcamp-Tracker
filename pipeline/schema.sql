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
    item_image VARCHAR NOT NULL,
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
