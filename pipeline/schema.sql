-- This file should contain table definitions for the database.

DROP TABLE IF EXISTS sale_event;
DROP TABLE IF EXISTS country;
DROP TABLE IF EXISTS album_genre;
DROP TABLE IF EXISTS genre;
DROP TABLE IF EXISTS album;
DROP TABLE IF EXISTS artist;


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

CREATE TABLE album(
    album_id INT GENERATED ALWAYS AS IDENTITY,
    album_name VARCHAR NOT NULL UNIQUE,
    artist_id INT NOT NULL UNIQUE,
    PRIMARY KEY (album_id),
    FOREIGN KEY (artist_id) REFERENCES artist(artist_id) ON DELETE CASCADE
);

CREATE TABLE genre(
    genre_id SMALLINT GENERATED ALWAYS AS IDENTITY,
    genre VARCHAR NOT NULL UNIQUE,
    PRIMARY KEY (genre_id)
);

CREATE TABLE album_genre(
    album_genre_id INT GENERATED ALWAYS AS IDENTITY,
    album_id INT NOT NULL,
    genre_id SMALLINT NOT NULL,
    PRIMARY KEY (album_genre_id),
    FOREIGN KEY (album_id) REFERENCES album(album_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genre(genre_id) ON DELETE CASCADE
);


CREATE TABLE sale_event(
    sale_id BIGINT GENERATED ALWAYS AS IDENTITY,
    sale_time TIMESTAMPTZ NOT NULL,
    amount INT NOT NULL,
    album_id INT NOT NULL,
    country_id SMALLINT NOT NULL,
    PRIMARY KEY (sale_id),
    FOREIGN KEY (album_id) REFERENCES album(album_id),
    FOREIGN KEY (country_id) REFERENCES country(country_id)
);
