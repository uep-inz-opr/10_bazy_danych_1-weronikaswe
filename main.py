# import pymysql
import csv
import sqlite3

sqlite_con = sqlite3.connect(':memory:', detect_types=sqlite3.PARSE_DECLTYPES|sqlite3.PARSE_COLNAMES)
cur = sqlite_con.cursor()

cur.execute('''CREATE TABLE polaczenia (from_subscriber data_type INTEGER, 
                  to_subscriber data_type INTEGER, 
                  datetime data_type timestamp, 
                  duration data_type INTEGER , 
                  celltower data_type INTEGER);''')

if __name__ == "__main__":
    # calls_dict_sum = dict()

    with open('polaczenia_duze.csv', 'r') as fin:
        reader = csv.reader(fin, delimiter=";")
        headers = next(reader)
        rows = [x for x in reader]
        cur.executemany("INSERT INTO polaczenia(from_subscriber, to_subscriber, datetime, duration, celltower) VALUES (?, ?, ?, ?, ?);",rows)
        sqlite_con.commit()
        # for row in reader:
        #     call_duration = int(row[3])
            # calls_dict_sum += call_duration
            # wszystko = sum(calls_dict_sum)


class ReportGenerator:
    def __init__(self, connection, escape_string="(%s)"):
        self.connection = connection
        # self.report_text = None
        self.escape_string = escape_string

    def generate_report(self, user_id):
        cursor = self.connection.cursor()
        args = (user_id,)
        sql = f"Select sum(duration) from polaczenia where from_subscriber = {self.escape_string}"
        # sql = f"-- Select * from polaczenia where from_subscriber={self.escape_string}"
        cursor.execute(sql, args)
        result = cursor.fetchone()[0]
        self.report_text = f"łączny czas trwania dla użytkownika {user_id} to {result}"

    def get_report(self):
        return self.report_text

rg=ReportGenerator(sqlite_con, escape_string="?")
rg.generate_report(283)
rg.get_report()

print(rg.get_report())

