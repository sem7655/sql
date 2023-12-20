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


def compare_records(sqlite_row, postgres_row):
    for field_index, field_name in enumerate(sqlite_row.keys()):
        assert sqlite_row[field_index] == postgres_row[field_name]


def test_count_matches(sqlite_conn, postgres_conn):
    for table_name in ["genre", "film_work", "person", "genre_film_work", "person_film_work"]:
        sqlite_count = count_rows_in_sqlite(table_name, sqlite_conn)
        postgres_count = count_rows_in_postgresql(table_name, postgres_conn)
        assert sqlite_count == postgres_count


def test_all_records_match(sqlite_conn, postgres_conn):
    for table_name in ["genre", "film_work", "person", "genre_film_work", "person_film_work"]:
        sqlite_rows = get_all_rows_from_sqlite(table_name, sqlite_conn)
        postgres_rows = get_all_rows_from_postgresql(table_name, postgres_conn)
        for sqlite_row, postgres_row in zip(sqlite_rows, postgres_rows):
            compare_records(sqlite_row, postgres_row)
