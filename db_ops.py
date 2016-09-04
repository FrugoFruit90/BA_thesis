import csv
import psycopg2
import sys
from datetime import datetime as dt
import math
import pandas
import pickle
import sqlalchemy
import time

DEBUG = False
TABLE_NAME = "postgres.public.chess_data"
DB_CONN = psycopg2.connect("dbname=postgres user=postgres password=admin")


def create_db_table(cursor, table_name):
    # here table schema should be defined
    cursor.execute('''CREATE TABLE ''' + table_name + '''
             (date TEXT, nameW TEXT, nameB TEXT, whiteRank INT, blackRank INT, tournament TEXT, t_round INT, result REAL);''')


def load_data_into_db(cursor, table_name):
    print('Data loading initiated\n')
    with open('./base.csv', newline='', encoding='ISO-8859-1') as csvfile:
        data = csv.reader(csvfile, delimiter=',')
        i = 0
        for row in data:
            # if we're debugging just load some chunk of data
            if DEBUG and i >= 1000:
                break

            try:
                date = dt.strptime(row[8], '%d.%m.%Y')
            except ValueError:
                # if date is malformed - ignore it
                continue

            nameW = row[0] + ' ' + row[1]
            nameB = row[3] + ' ' + row[4]
            whiteRank = int(row[2])
            blackRank = int(row[5])
            result = float(row[9])

            tournament_data = row[7].split(sep=' ')
            try:
                tournament = ' '.join(tournament_data[:-1])
                t_round = int(math.floor(float(tournament_data[-1][1:-1])))
            except:
                t_round = None
                tournament = ' '.join(tournament_data)

            db_row = (date, nameW, nameB, whiteRank, blackRank, tournament, t_round, result)
            # last argument is to be calculated later
            if insert_into_db(cursor, db_row):
                i += 1
        sys.stdout.write('\rloaded %s rows' % (i))
        sys.stdout.flush()
        DB_CONN.commit()
    sys.stdout.write('\nDone loading data into DB\n')


if __name__ == '__main__':
    c = DB_CONN.cursor()
    create_db_table(c, TABLE_NAME)
    print("Table created!")
    load_data_into_db(c, TABLE_NAME)
    queries = [
        "ALTER TABLE chess_data ALTER COLUMN date TYPE DATE using to_date(date, 'YYYY-MM-DD')",
        "SELECT x.* INTO players FROM (SELECT DISTINCT namew FROM chess_data2 " \
        "UNION SELECT DISTINCT nameb FROM chess_data2) x",
        "SELECT DISTINCT tournament INTO tournaments FROM chess_data2"
        "ALTER TABLE players ADD COLUMN id SERIAL PRIMARY KEY;"
        "ALTER TABLE tournaments ADD COLUMN id SERIAL PRIMARY KEY;"
        "SELECT * INTO chess_data2 FROM chess_data",
        "ALTER TABLE chess_data2 ADD COLUMN id SERIAL PRIMARY KEY",
        "ALTER TABLE chess_data2 ADD COLUMN tournament_id INTEGER",
        "ALTER TABLE chess_data2 ADD COLUMN namew_id INTEGER",
        "ALTER TABLE chess_data2 ADD COLUMN nameb_id INTEGER",
        "UPDATE chess_data2 cd2 SET tournament_id = (SELECT id FROM tournaments WHERE t_name = cd2.tournament)",
        "UPDATE chess_data2 cd2 SET namew_id = (SELECT id FROM players WHERE name = cd2.namew)",
        "UPDATE chess_data2 cd2 SET nameb_id = (SELECT id FROM players WHERE name = cd2.nameb)",
        "ALTER TABLE chess_data2 DROP COLUMN namew",
        "ALTER TABLE chess_data2 DROP COLUMN nameb",
        "ALTER TABLE chess_data2 DROP COLUMN tournament"
        "CREATE INDEX nameb__index ON public.chess_data2 (nameb_id, tournament_id, date)",
        "CREATE INDEX namew__index ON public.chess_data2 (namew_id, tournament_id, date)",

        "SELECT *,"
            # Number of points that the white player has so far accrued throughout the tournament
        "(SELECT coalesce(SUM(result),0) from chess_data2 t2 " \
        "where (t1.namew_id = t2.namew_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90 ) + (SELECT coalesce(SUM(1-result),0) from chess_data2 t2 " \
        "where (t1.namew_id = t2.nameb_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90 ) AS result_in_t_w, "
            # Number of points that the black player has so far accrued throughout the tournament
        "(SELECT coalesce(SUM(result),0) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.namew_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90 ) + (SELECT coalesce(SUM(1-result),0) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.nameb_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90 ) AS result_in_t_b, " \
            # Number of games that the white player has so far played in the tournament
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.namew_id = t2.namew_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90) + (SELECT count(*) from chess_data2 t2 " \
        "where (t1.namew_id = t2.nameb_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90) AS games_t_w, " \
            # Number of games that the black player has so far played in the tournament
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.namew_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90) + (SELECT count(*) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.nameb_id) and t1.tournament_id = t2.tournament_id and t1.date > t2.date " \
        "and t1.date < t2.date + 90) AS games_t_b, " \
            # Number of games that the white player has so far played in the whole career
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.namew_id = t2.namew_id) and t1.date > t2.date) " \
        "+ (SELECT count(*) from chess_data2 t2 " \
        "where (t1.namew_id = t2.nameb_id) and t1.date > t2.date) " \
        "AS games_c_w, " \
            # Number of games that the black player has so far played in the whole career
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.namew_id) and t1.date > t2.date) " \
        "+ (SELECT count(*) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.nameb_id) and t1.date > t2.date) " \
        "AS games_c_b, " \
            # Number of games with white that the white player has played during last 6 months
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.namew_id = t2.namew_id) and t1.date > t2.date and t1.date < t2.date + 180) " \
        "AS games_6m_w, " \
            # Number of games with black that the black player has played during last 6 months
        "(SELECT count(*) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.nameb_id) AND t1.date > t2.date AND t1.date < t2.date + 180) " \
        "AS games_6m_b, " \
            # Number of points with white that the white player has played during last 6 months
        "(SELECT coalesce(SUM(result),0) from chess_data2 t2 " \
        "where (t1.namew_id = t2.namew_id) and t1.date > t2.date " \
        "and t1.date < t2.date + 180 ) AS result_6m_w, " \
            # Number of points with black that the black player has played during last 6 months
        "(SELECT coalesce(SUM(1-result),0) from chess_data2 t2 " \
        "where (t1.nameb_id = t2.nameb_id) and t1.date > t2.date " \
        "and t1.date < t2.date + 180 ) AS result_6m_b " \

        # Close the query
        "INTO aaaa from chess_data2 t1"
    ]

    a = 0
    print("Starting the queries!")
    for query in queries:
        start = time.time()
        c.execute(query)
        a += 1
        print("Completed query number", a)
        DB_CONN.commit()
        end = time.time()
        print(end - start)
        print('Done feature engineering!')
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
    X = pandas.read_sql_table('aaaa', engine)
    DB_CONN.close()
    with open('df_X', 'wb') as f:
        pickle.dump(X, f, pickle.HIGHEST_PROTOCOL)
    print("Data dumped!")
    sys.exit(0)

    # Number of years in database that white player has
    # TODO get rid of "OR"
    # "(SELECT t1.date - t2.date "
    # "FROM chess_data2 t2 "
    # "WHERE t2.date = (SELECT CASE WHEN "
    #     "(SELECT min(date) FROM chess_data2 t3 "
    #         "WHERE t3.date < t1.date AND t1.namew_id = t3.namew_id LIMIT 1) "
    #     "> " \
    #     "(SELECT min(date) FROM chess_data2 t3 "
    #         "WHERE t3.date < t1.date AND t1.namew_id = t3.nameb_id LIMIT 1) "
    # "THEN"
    #     "(SELECT min(date) FROM chess_data2 t3 "
    #         "WHERE t3.date < t1.date AND t1.namew_id = t3.namew_id LIMIT 1) "
    # "ELSE "
    #     "(SELECT min(date) FROM chess_data2 t3 "
    #         "WHERE t3.date < t1.date AND t1.namew_id = t3.namew_id LIMIT 1) "
    # "END) LIMIT 1) AS white_exp "
    # Number of years in database that black player has
    # TODO get rid of "OR"
    # "CASE WHEN t1.games_t_b=0 THEN 0"
    # "CASE WHEN t1.games_t_b=1 THEN ("
    # "SELECT "
    # "SELECT max() FROM "
    # "(SELECT t1.date - t2.date FROM chess_data2 t2 "
    # "WHERE t2.date = (SELECT max(date) FROM chess_data2 t3 WHERE t3.date < t1.date AND " \
    # "(t1.nameb_id = t3.namew_id OR t1.nameb_id = t3.nameb_id)) Limit 1) AS black_exp "
    # TODO Last game result for white
    # TODO Last game result for black