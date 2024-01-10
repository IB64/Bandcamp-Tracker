from os import environ

from psycopg2 import extensions, connect
from dotenv import load_dotenv


def get_db_connection() -> extensions.connection:
    """
    Returns a connection to the AWS Bandcamp database
    """

    try:
        return connect(user=environ["DB_USER"],
                       password=environ["DB_PASSWORD"],
                       host=environ["DB_IP"],
                       port=environ["DB_PORT"],
                       database=environ["DB_NAME"])
    except ConnectionError:
        print("Error: Cannot connect to the database")


if __name__ == "__main__":
    cur = get_db_connection()
    load_dotenv()
    cur.execute("SELECT * FROM genre ORDER BY genre_id;")

    genres = cur.fetchall()
    genres = {r[1]: r[0] for r in genres}
    print(genres)
