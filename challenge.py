import cx_Oracle
import sys
import os
import sheets_api
import json
import datetime


class cyrela_database_automation:

    def __init__(self):

        # Start oracle client
        try:
            if sys.platform.startswith("darwin"):
                lib_dir = os.path.join(os.environ.get("HOME"), "Downloads",
                                       "instantclient_19_8")
                cx_Oracle.init_oracle_client(lib_dir=lib_dir)
                cx_Oracle.init_oracle_client(lib_dir=r"C:\oracle\instantclient_19_9")
        except Exception as err:
            print("Whoops!")
            print(err);
            sys.exit(1);

        # Credentials
        hostname = 'oracle.fiap.com.br'
        port = 1521
        servicename = 'orcl'
        username = input('Username: ')
        password = input('Password')
        dsn_tns = cx_Oracle.makedsn(hostname, port, servicename)
        conn = cx_Oracle.connect(user=username, password=password, dsn=dsn_tns)

        # Connect to Database
        self.database = conn.cursor()

        # All tables names
        self.tables = ["POSICAOFINANCEIRA", "PARCELA", "CONTROLESESSAO", "COOBRIGADO", "LOG_NAVEGACAO"]

        # List of all columns titles
        self.tilles = []

        # Onganize all datas before sends to sheets
        self.to_sheets = []

    def database_sync_sheets(self, table):

        try:

            # SQL command
            data = self.database.execute(f"SELECT * FROM {table}")

            # connect to google sheets
            sheet = sheets_api.client.open('Copy of Cyrela Database').worksheet(table)

            # Get all Titles
            for title in data.description:
                self.tilles.append(title[0])

            # Sends colunms titles to Google Sheet
            sheet.insert_row(self.tilles)
            self.tilles.clear()

            # Get all rows
            rows = data.fetchall()

            # Prepare query to Google Sheet
            for row in rows:
                line = []

                # Create a new list for each line
                for item in row:

                    # If item is a date, makes datetime type serializable
                    if type(item) == datetime.datetime:
                        line.append(json.dumps(item, indent=4, sort_keys=True, default=str).strip('"'))
                    else:
                        line.append(item)

                # Onganize all lines before sends to sheets
                self.to_sheets.append(line)

            # Send all lines to Google Sheets
            sheet.insert_rows(self.to_sheets, 2)
            self.to_sheets.clear()
            line.clear()

            print(f"{table} has been insert on sheets!")

        except Exception as e:
            print(e)

database = cyrela_database_automation()

for table in database.tables:
    database.database_sync_sheets(table)
