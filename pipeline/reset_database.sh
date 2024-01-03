source .env
export PGPASSWORD=$DB_PASSWORD
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "TRUNCATE TABLE sale_event, album_genre, genre, album, artist;"