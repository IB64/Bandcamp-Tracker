source .env
export PGPASSWORD=$DB_PASSWORD
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM sale_event;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM item_genre;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM genre;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM item;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM item_type;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM artist;"
psql -h $DB_IP -p $DB_PORT -U $DB_USER -c "DELETE FROM country;"