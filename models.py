import os

from dotenv import load_dotenv

load_dotenv()

import sqlite3

from contextlib import closing

from datetime import datetime, date, time

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

import uuid
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Film_work:
    title: str
    description: str
    type: str
    creation_date: datetime
    created: datetime
    modified: datetime
    rating: float = field(default=0.0)
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Person:
    full_name: str
    created: datetime
    modified: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Genre:
    name: str
    description: str
    created: datetime
    modified: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Genre_film_work:
    created: datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    genre_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass(frozen=True)
class Person_film_work:
    created: datetime
    role: str
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    person_id: uuid.UUID = field(default_factory=uuid.uuid4)
    film_work_id: uuid.UUID = field(default_factory=uuid.uuid4)


movie = Film_work(title='movie1', description='new movie', rating=0.0, type='fff', creation_date=datetime.today(),
                  created=datetime.now(), modified=datetime.now(), id=uuid.UUID('58725b1a-26ac-4bde-9658-ee4af98663cb'))
# print(movie)
# print(movie.title)

psycopg2.extras.register_uuid()


def save_film_work_to_postgres(conn: psycopg2.extensions.connection, film_works):
    conn1 = conn.cursor()
    for film_work in film_works:
        conn1.execute(
            "INSERT INTO content.film_work (id, title, description, creation_date, rating, type, created, modified) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s ) ON CONFLICT DO NOTHING",
            (film_work.id, film_work.title, film_work.description, film_work.creation_date, film_work.rating,
             film_work.type,
             film_work.created, film_work.modified))


def save_person_to_postgres(conn: psycopg2.extensions.connection, persons):
    conn1 = conn.cursor()
    for person in persons:
        conn1.execute("INSERT INTO content.person (id, full_name, created, modified) "
                      "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                      (person.id, person.full_name, person.created, person.modified))


def save_genre_to_postgres(conn: psycopg2.extensions.connection, genres):
    conn1 = conn.cursor()
    for genre in genres:
        conn1.execute("INSERT INTO content.genre (id, name, description, created, modified) "
                      "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                      (genre.id, genre.name, genre.description, genre.created, genre.modified))


def save_genre_film_work_to_postgres(conn: psycopg2.extensions.connection, genre_film_works):
    conn1 = conn.cursor()
    for genre_film_work in genre_film_works:
        conn1.execute("INSERT INTO content.genre_film_work (id, film_work_id, genre_id, created) "
                      "VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING",
                      (genre_film_work.id, genre_film_work.film_work_id, genre_film_work.genre_id,
                       genre_film_work.created))


def save_person_film_work_to_postgres(conn: psycopg2.extensions.connection, person_film_works):
    conn1 = conn.cursor()
    for person_film_work in person_film_works:
        conn1.execute("INSERT INTO content.person_film_work (id, film_work_id, person_id, role, created) "
                      "VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                      (person_film_work.id, person_film_work.film_work_id, person_film_work.person_id,
                       person_film_work.role, person_film_work.created))


def load_movies_from_sqlite(connection: sqlite3.Connection, conn, n):
    movies_list = []
    cur = connection.cursor()
    cur.execute("select * from film_work")
    while True:
        rows = cur.fetchmany(n)
        if rows:
            movies_list.clear()
            for i in range(len(rows)):
                movie = Film_work(title=rows[i][1], description=rows[i][2], rating=rows[i][5], type=rows[i][6],
                                  creation_date=rows[i][3],
                                  created=rows[i][7], modified=rows[i][8],
                                  id=uuid.UUID(rows[i][0]))
                movies_list.append(movie)
            save_film_work_to_postgres(conn, movies_list)
        else:
            break


def load_genres_from_sqlite(connection: sqlite3.Connection, conn, n):
    genre_list = []
    cur = connection.cursor()
    cur.execute("select * from genre")
    while True:
        rows = cur.fetchmany(n)
        if rows:
            genre_list.clear()
            for i in range(len(rows)):
                genre = Genre(name=rows[i][1], description=rows[i][2],
                              created=rows[i][3], modified=rows[i][4],
                              id=uuid.UUID(rows[i][0]))
                genre_list.append(genre)
            save_genre_to_postgres(conn, genre_list)
        else:
            break


def load_persons_from_sqlite(connection: sqlite3.Connection, conn, n):
    person_list = []
    cur = connection.cursor()
    cur.execute("select * from person")
    while True:
        rows = cur.fetchmany(n)
        if rows:
            person_list.clear()
            for i in range(len(rows)):
                person = Person(full_name=rows[i][1], created=rows[i][2],
                                modified=rows[i][3], id=uuid.UUID(rows[i][0]))
                person_list.append(person)
            save_person_to_postgres(conn, person_list)
        else:
            break


def load_genre_film_work_from_sqlite(connection: sqlite3.Connection, conn, n):
    genre_film_work_list = []
    cur = connection.cursor()
    cur.execute("select * from genre_film_work")
    while True:
        rows = cur.fetchmany(n)
        if rows:
            genre_film_work_list.clear()
            for i in range(len(rows)):
                genre_film_work = Genre_film_work(film_work_id=uuid.UUID(rows[i][1]), created=rows[i][3],
                                                  genre_id=uuid.UUID(rows[i][2]), id=uuid.UUID(rows[i][0]))
                genre_film_work_list.append(genre_film_work)
            save_genre_film_work_to_postgres(conn, genre_film_work_list)
        else:
            break


def load_person_film_work_from_sqlite(connection: sqlite3.Connection, conn, n):
    person_film_work_list = []
    cur = connection.cursor()
    cur.execute("select * from person_film_work")
    while True:
        rows = cur.fetchmany(n)
        if rows:
            person_film_work_list.clear()
            for i in range(len(rows)):
                person_film_work = Person_film_work(film_work_id=uuid.UUID(rows[i][1]), created=rows[i][4],
                                                    person_id=uuid.UUID(rows[i][2]), role=rows[i][3],
                                                    id=uuid.UUID(rows[i][0]))
                person_film_work_list.append(person_film_work)
            save_person_film_work_to_postgres(conn, person_film_work_list)
        else:
            break


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    n = 5
    load_movies_from_sqlite(connection, pg_conn, n)
    load_genres_from_sqlite(connection, pg_conn, n)
    load_persons_from_sqlite(connection, pg_conn, n)
    load_genre_film_work_from_sqlite(connection, pg_conn, n)
    load_person_film_work_from_sqlite(connection, pg_conn, n)
    pg_conn.commit()


if __name__ == '__main__':
    dsl = {'dbname': os.environ.get('DB_NAME'), 'user': os.environ.get('DB_USER'),
           'password': os.environ.get('DB_PASSWORD'), 'host': os.environ.get('DB_HOST', '127.0.0.1'),
           'port': os.environ.get('DB_PORT', 5432)}
    with closing(psycopg2.connect(**dsl, cursor_factory=DictCursor)) as pg_conn:
        with sqlite3.connect('db.sqlite') as sqlite_conn:
            load_from_sqlite(sqlite_conn, pg_conn)
        sqlite_conn.close()
