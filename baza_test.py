import pytest
import sqlite3
from psycopg2 import connect, OperationalError
import os
from models import Film_work, Person, Genre, Genre_film_work, Person_film_work


def connect_to_sqlite():
    try:
        conn = sqlite3.connect("db.sqlite")
        return conn
    except OperationalError:
        pytest.fail("Failed to connect to SQLite database")


def connect_to_postgresql():
    try:
        conn = connect(database=os.environ["DB_NAME"], user=os.environ["DB_USER"], password=os.environ["DB_PASSWORD"])
        return conn
    except OperationalError:
        pytest.fail("Failed to connect to PostgreSQL database")


@pytest.fixture(scope="module")
def sqlite_conn():
    conn = connect_to_sqlite()
    yield conn
    conn.close()


@pytest.fixture(scope="module")
def postgres_conn():
    conn = connect_to_postgresql()
    yield conn
    conn.close()


def count_rows_in_sqlite(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    return count


def count_rows_in_postgresql(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT COUNT(*) FROM content.{table_name}")
    count = cursor.fetchone()[0]
    return count


def get_all_rows_from_sqlite(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM {table_name}")
    rows = cursor.fetchall()
    return rows


def get_all_rows_from_postgresql(table_name, conn):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM content.{table_name}")
    rows = cursor.fetchall()
    return rows


def compare_film_work_records(sqlite_row, postgres_row):
    assert sqlite_row[0] == postgres_row["id"]
    assert sqlite_row[1] == postgres_row["title"]
    assert sqlite_row[2] == postgres_row["description"]
    assert sqlite_row[5] == postgres_row["rating"]
    assert sqlite_row[6] == postgres_row["type"]
    assert sqlite_row[7] == postgres_row["created"]
    assert sqlite_row[8] == postgres_row["modified"]


def compare_person_records(sqlite_row, postgres_row):
    assert sqlite_row[0] == postgres_row["id"]
    assert sqlite_row[1] == postgres_row["full_name"]
    assert sqlite_row[2] == postgres_row["created"]
    assert sqlite_row[3] == postgres_row["modified"]


def compare_genre_records(sqlite_row, postgres_row):
    assert sqlite_row[0] == postgres_row["id"]
    assert sqlite_row[1] == postgres_row["name"]
    assert sqlite_row[2] == postgres_row["description"]
    assert sqlite_row[3] == postgres_row["created"]
    assert sqlite_row[4] == postgres_row["modified"]


def compare_genre_film_work_records(sqlite_row, postgres_row):
    assert sqlite_row[0] == postgres_row["id"]
    assert sqlite_row[1] == postgres_row["film_work_id"]
    assert sqlite_row[2] == postgres_row["genre_id"]
    assert sqlite_row[3] == postgres_row["created"]


def compare_person_film_work_records(sqlite_row, postgres_row):
    assert sqlite_row[0] == postgres_row["id"]
    assert sqlite_row[1] == postgres_row["film_work_id"]
    assert sqlite_row[2] == postgres_row["person_id"]
    assert sqlite_row[3] == postgres_row["role"]
    assert sqlite_row[4] == postgres_row["created"]


# Test film_work table count
def test_film_work_count_matches(sqlite_conn, postgres_conn):
    sqlite_film_work_count = count_rows_in_sqlite("film_work", sqlite_conn)
