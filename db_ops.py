import csv
import psycopg2
import sys
from datetime import datetime as dt
import math
DEBUG = True
TABLE_NAME = "chess_data"
DB_CONN = psycopg2.connect("dbname=postgres user=postgres")


def create_db_table(cursor, table_name):
    # here table schema should be defined
    cursor.execute('''CREATE TABLE ''' + table_name + '''
             (date TEXT, nameW TEXT, nameB TEXT, whiteRank INT, blackRank INT, tournament TEXT, t_round INT, result REAL, t_result REAL NULLABLE);''')


def check_if_table_exists(cursor, table_name):
    return len(cursor.execute('''SELECT name FROM sqlite_master WHERE type='table' AND name=?;''', (table_name,)).fetchall()) > 0


def insert_into_db(cursor, data_row):
    try:
        cursor.execute("INSERT INTO chess_data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", data_row)
        return True
    except Exception:
        return False


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

            db_row = (date, nameW, nameB, whiteRank, blackRank, tournament, t_round, result,
                      None)  # last argument is to be calculated later
            if insert_into_db(cursor, db_row):
                i += 1
            DB_CONN.commit()
            sys.stdout.write('\rloaded %s rows' % (i))
            sys.stdout.flush()

    sys.stdout.write('\nDone loading data into DB\n')


if __name__ == '__main__':
    c = DB_CONN.cursor()
    if not check_if_table_exists(c, TABLE_NAME):
        create_db_table(c, TABLE_NAME)
    load_data_into_db(c, TABLE_NAME)
    DB_CONN.close()
    print('Done!')
    sys.exit(0)
