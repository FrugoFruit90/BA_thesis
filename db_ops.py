import csv
import psycopg2
import sys
from datetime import datetime as dt
import math
import pandas
import pickle
import sqlalchemy

DEBUG = False
TABLE_NAME = "postgres.public.chess_data"
DB_CONN = psycopg2.connect("dbname=postgres user=postgres password=admin")


def create_db_table(cursor, table_name):
    # here table schema should be defined
    cursor.execute('''CREATE TABLE ''' + table_name + '''
             (date TEXT, nameW TEXT, nameB TEXT, whiteRank INT, blackRank INT, tournament TEXT, t_round INT, result REAL);''')


def check_if_table_exists(cursor, table_name):
    cursor.execute("SELECT EXISTS(SELECT * FROM information_schema.tables where table_name=%s)", ('table_name',))
    return cursor.fetchone()[0]


def insert_into_db(cursor, data_row):
    query = "INSERT INTO chess_data(date, namew, nameb, whiterank, blackrank, tournament, t_round, result) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor.execute(query, data_row)
    return True


def add_column(cursor):
    query = "ALTER TABLE chess_data ADD COLUMN career_games_b REAL "
    cursor.execute(query)
    return True


def fill_col_data(cursor):
    query = "update chess_data t1 set career_games_b = (select coalesce(count(nameW), 0) from chess_data t2 " \
            "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.date > t2.date)"
    cursor.execute(query)
    return True


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
    # create_db_table(c, TABLE_NAME)
    # load_data_into_db(c, TABLE_NAME)
    # add_column(c)
    # fill_col_data(c)
    # queries = [
    #     "ALTER TABLE chess_data ALTER COLUMN date TYPE DATE using to_date(date, 'YYYY-MM-DD')",
    #     "ALTER TABLE chess_data ADD COLUMN result_in_t_w real",
    #     "update chess_data t1 set result_in_t_w = (select coalesce(SUM(CASE WHEN t1.nameW = t2.nameW THEN result ELSE 1-result END),0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.tournament = t2.tournament and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN result_in_t_b real",
    #     "update chess_data t1 set result_in_t_b = (select coalesce(SUM(CASE WHEN t1.nameB = t2.nameW THEN result ELSE 1-result END), 0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.tournament = t2.tournament and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN games_in_t_w real",
    #     "update chess_data t1 set games_in_t_w = (select coalesce(count(nameW),0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.tournament = t2.tournament and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN games_in_t_b real",
    #     "update chess_data t1 set games_in_t_b = (select coalesce(count(nameB),0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.tournament = t2.tournament and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN career_games_w int",
    #     "update chess_data t1 set career_games_w = (select coalesce(count(nameW),0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN career_games_b int",
    #     "update chess_data t1 set career_games_b = (select coalesce(count(nameW), 0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN career_w_record real",
    #     "update chess_data t1 set career_w_record = (select coalesce(SUM(CASE WHEN t1.nameW = t2.nameW THEN result ELSE 1-result END), 0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN career_b_record real",
    #     "update chess_data t1 set career_b_record = (select coalesce(SUM(CASE WHEN t1.nameB = t2.nameW THEN result ELSE 1-result END), 0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.date > t2.date)",
    #     "ALTER TABLE chess_data ADD COLUMN last_q_record_w real",
    #     "update chess_data t1 set last_q_record_w = (select coalesce(SUM(CASE WHEN t1.nameW = t2.nameW THEN result ELSE 1-result END), 0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.date - t2.date<90 and t1.date - t2.date>0)",
    #     "ALTER TABLE chess_data ADD COLUMN last_q_games_w int",
    #     "update chess_data t1 set last_q_games_w = (select coalesce(count(nameW),0) from chess_data t2 " \
    #     "where (t1.nameW = t2.nameW or t1.nameW = t2.nameB) and t1.date - t2.date<90 and t1.date - t2.date>0)",
    #     "ALTER TABLE chess_data ADD COLUMN last_q_record_b real",
    #     "update chess_data t1 set last_q_record_b = (select coalesce(SUM(CASE WHEN t1.nameB = t2.nameW THEN result ELSE 1-result END), 0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.date - t2.date<90 and t1.date - t2.date>0)",
    #     "ALTER TABLE chess_data ADD COLUMN last_q_games_b int",
    #     "update chess_data t1 set last_q_games_b = (select coalesce(count(nameW),0) from chess_data t2 " \
    #     "where (t1.nameB = t2.nameW or t1.nameB = t2.nameB) and t1.date - t2.date<90 and t1.date - t2.date>0)",
    #     "ALTER TABLE chess_data ADD COLUMN last_game_result_w real",
    #     "update chess_data t1 set last_game_result_w = (select coalesce(CASE WHEN t1.nameW = t2.nameW THEN t2.result ELSE t2.result END,0) from chess_data t2 " \
    #     " WHERE t2.date = (select max(date) from chess_data t3 where t3.date < t1.date AND" \
    #     " (t1.nameW = t3.nameW or t1.nameW = t3.nameB)) AND (t1.nameW = t2.nameW OR t1.nameW = t2.nameB) Limit 1)",
    #     "update chess_data t1 set last_game_result_w = (SELECT coalesce(last_game_result_w, 0))",
    #     "ALTER TABLE chess_data ADD COLUMN last_game_result_b real",
    #     "update chess_data t1 set last_game_result_b = (select coalesce(CASE WHEN t1.nameB = t2.nameW THEN t2.result ELSE t2.result END,0) from chess_data t2 " \
    #     " WHERE t2.date = (select max(date) from chess_data t3 where t3.date < t1.date AND" \
    #     " (t1.nameB = t3.nameW or t1.nameB = t3.nameB)) AND (t1.nameB = t2.nameW OR t1.nameB = t2.nameB) Limit 1)",
    #     "update chess_data t1 set last_game_result_b = (SELECT coalesce(last_game_result_b, 0))",
    # ]
    # a = 0
    # for query in queries:
    #     c.execute(query)
    #     a += 1
    #     print("Completed query number", a)
    #     DB_CONN.commit()
    # print('Done feature engineering!')
    # engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')
    engine = sqlalchemy.create_engine("postgresql+psycopg2://postgres:postgres@localhost:5432/postgres")
    X = pandas.read_sql_table('chess_data', engine)
    DB_CONN.close()
    with open('df_X', 'wb') as f:
        pickle.dump(X, f, pickle.HIGHEST_PROTOCOL)
    print("Data dumped!")
    sys.exit(0)
