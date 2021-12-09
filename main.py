import collections
import os, pyodbc, csv
from typing import NamedTuple
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import String

CONN_STR = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"


DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")

col_names = [
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "validity_number",
    "cause_code",
    "earthquake_magnitud",
    "deposits_num",
    "country",
    "location",
    "latitude",
    "longitude",
    "max_weater_height",
    "runups_num",
    "iida_magnitud",
    "intensity",
    "total_deaths",
    "total_missing",
    "total_missing_desc",
    "total_injuries",
    "total_damage",
    "total_damage_desc",
    "total_houses_destroyed",
    "total_houses_damaged",
]
ColNamesTuple = collections.namedtuple("col_names", col_names)


class EtlHandler:
    def get_conn_str(self):
        return CONN_STR.format(
            server="db",
            database="tempdb",
            username="sa",
            password=DB_PASSWORD,
        )

    def create_connection(self):
        return pyodbc.connect(self.get_conn_str())

    def create_tsunami(self, cursor, event_id, cols: ColNamesTuple):
        cursor.execute(
            f"""
            insert into Tsunami(iida_magnitud,earthquake_magnitud, intensity, runups_num, deposits_num, max_weater_height, event_id)
            values ({cols.iida_magnitud},{cols.earthquake_magnitud}, {cols.intensity}, {cols.runups_num}, {cols.deposits_num},{cols.max_weater_height}, {event_id})
        """
        )

    def get_tsunami_fields(self,cols: ColNamesTuple):
        d = {}
        d.setdefault("iida_magnitud", col_names.iida_magnitud or None)
        return d

    def create_information(self, pathfile):
        conn = self.create_connection()
        cursor = conn.cursor()
        with open(pathfile, mode="r") as file:
            index = 0
            for item in csv.reader(file):
                if index > 1:
                    cols = ColNamesTuple(*item)
                    self.create_tsunami(
                        cursor,
                        1,
                        cols,
                    )

                    break
                index += 1

    def create_table_location(self, cursor):
        cursor.execute("drop table if exists Location")
        cursor.execute(
            """
        create table Location(
            id int primary key,
            country text,
            location text,
            latitud float,
            longitud text,
        )
        """
        )

    def create_table_event(self, cursor):
        cursor.execute("drop table if exists Event")
        cursor.execute(
            """
        create table Event(
            id int primary key,
            register_at datetime,
            cause_code text
        )
        """
        )

    def create_table_tsunami(self, cursor):
        cursor.execute("drop table if exists Tsunami;")
        cursor.execute(
            """
        create table Tsunami(
            iida_magnitud float null,
            earthquake_magnitud float null,
            intensity float null,
            runups_num integer null,
            deposits_num integer null,
            max_weater_height float null,
            event_id int,
        );
        """
        )

    def create_table_damage(self, cursor):
        cursor.execute("drop table if exists Damage;")
        cursor.execute(
            """
        create table Damage(
            total_deaths int null,
            total_missing int null,
            total_missing_desc text null,
            total_injuries int null,
            total_damage float null,
            total_damage_desc text null,
            total_houses_destroyed int,
            total_houses_damaged int,

            tsunami_id int,
        );
        """
        )

    def create_models(
        self,
    ):
        conn = self.create_connection()
        cursor = conn.cursor()

        self.create_table_location(cursor)
        self.create_table_tsunami(cursor)
        self.create_table_event(cursor)
        self.create_table_damage(cursor)

        conn.commit()

    def make_querys(
        self,
    ):
        print("done")

    def create_alch(self):
        engine = create_engine(
            "mssql+pyodbc://{user}:{paswword}@{host}:1433/{dbname}?driver=ODBC+Driver+17+for+SQL+Server".format(
                user="sa", password=DB_PASSWORD, host="db", dbname="tempdb"
            )
        )

        Base.metadata.create_all(bind=engine)


handler = EtlHandler()

# print(handler.get_conn_str())
# print(handler.create_connection())

running = True
while running:
    print(
        """
        MENU
        -----------------------------
        1. Crear modelo
        2. Crear información
        3. Realizar consultas
        4. Salir
        -----------------------------
    """
    )
    option = input("Selecciona una opción:")
    if option == "4":
        running = False
    elif option == "1":
        handler.create_models()
    elif option == "2":
        pathfile = input("Ingrese ruta absoluta del archivo:")
        handler.create_information(pathfile or "/workspace/tsunamies.csv")
    elif option == "3":
        handler.make_querys()
    else:
        print("OPción no reconocida")
