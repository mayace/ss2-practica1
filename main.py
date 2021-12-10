import os, pyodbc, csv, datetime, collections, pandas


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

    def create_tsunami(self, cursor, **kwargs):
        result = cursor.execute(
            """
            insert into Tsunami(iida_magnitud,earthquake_magnitud, intensity, runups_num, deposits_num, max_weater_height, event_id)
            output inserted.id
            values ({iida_magnitud},{earthquake_magnitud}, {intensity}, {runups_num}, {deposits_num},{max_weater_height}, {event_id})
        """.format(
                **kwargs
            )
        )

        for (id,) in result:
            return id

    def create_damage(self, cursor, **kwargs):
        result = cursor.execute(
            """
            insert into Damage(total_deaths,total_missing, total_missing_desc, total_injuries, total_damage, total_damage_desc, total_houses_destroyed,total_houses_damaged, tsunami_id)
            output inserted.id
            values ({total_deaths},{total_missing}, '{total_missing_desc}', {total_injuries}, {total_damage},'{total_damage_desc}', {total_houses_destroyed},{total_houses_damaged},{tsunami_id})
        """.format(
                **kwargs
            )
        )

        for (id,) in result:
            return id

    def get_or_create_location(self, cursor, country=None, location=None):
        result = cursor.execute(
            "select id from Location where country like ? and location like ?;",
            (country, location),
        )

        for (id,) in result:
            return id

        sql = """
            insert into Location(country, location)
            output inserted.id
            values (?, ?);
        """

        result = cursor.execute(sql, (country, location))

        for (id,) in result:
            return id

    def create_event(
        self,
        cursor,
        location=None,
        country=None,
        year=None,
        month=None,
        day=None,
        hour=None,
        minute=None,
        second=None,
        latitude=None,
        longitude=None,
        cause_code=None,
    ):
        sql = """
            insert into Event(year,month,day,hour,minute,second, location, country, latitude, longitude, cause_code)
            output inserted.id
            values (?,?,?,?,?,?, ?, ?, ?, ?, ?);
        """

        result = cursor.execute(
            sql,
            (
                year,
                month,
                day,
                hour,
                minute,
                second,
                location,
                country,
                latitude,
                longitude,
                cause_code,
            ),
        )

        for (id,) in result:
            return id

    def get_str_field(self, value):
        return value.strip().lower() if value else ""

    def get_float_field(self, value):
        return value.strip().lower() if value else "null"

    def parse_float(self, value):
        try:
            return float(value)
        except:
            return 0

    def parse_int(self, value):
        return int(self.parse_float(value))

    def get_tsunami_fields(self, cols: ColNamesTuple):
        d = {}
        d.setdefault("iida_magnitud", cols.iida_magnitud or "null")
        d.setdefault("earthquake_magnitud", cols.earthquake_magnitud or "null")
        d.setdefault("intensity", cols.intensity or "null")
        d.setdefault("runups_num", cols.runups_num or "null")
        d.setdefault("deposits_num", cols.deposits_num or "null")
        d.setdefault("max_weater_height", cols.max_weater_height or "null")
        return d

    def get_damage_fields(self, cols):
        d = {}
        d.setdefault("total_deaths", cols.total_deaths or "null")
        d.setdefault("total_missing", cols.total_missing or "null")
        d.setdefault("total_missing_desc", cols.total_missing_desc or "")
        d.setdefault("total_injuries", cols.total_injuries or "null")
        d.setdefault("total_damage", cols.total_damage or "null")
        d.setdefault("total_damage_desc", cols.total_damage_desc or "")
        d.setdefault("total_houses_destroyed", cols.total_houses_destroyed or "null")
        d.setdefault("total_houses_damaged", cols.total_houses_damaged or "null")
        return d

    def get_location_fields(self, cols):
        d = {}
        d.setdefault("country", self.get_str_field(cols.country))
        d.setdefault("location", self.get_str_field(cols.location))
        return d

    def get_event_fields(self, cols):
        d = {}
        d.setdefault("year", self.parse_int(cols.year))
        d.setdefault("month", self.parse_int(cols.month))
        d.setdefault("day", self.parse_int(cols.day))
        d.setdefault("minute", self.parse_int(cols.minute))
        d.setdefault("second", self.parse_int(cols.second))
        d.setdefault("cause_code", self.get_str_field(cols.cause_code))
        d.setdefault("latitude", self.parse_float(cols.latitude))
        d.setdefault("longitude", self.parse_float(cols.longitude))
        d.setdefault("location", self.get_str_field(cols.location))
        d.setdefault("country", self.get_str_field(cols.country))

        return d

    def create_information(self, pathfile):
        with open(pathfile, mode="r") as file:
            index = 0
            with self.create_connection() as conn:
                cursor = conn.cursor()
                for item in csv.reader(file):
                    if index > 1:
                        # print(item)
                        cols = ColNamesTuple(*item)

                        # location_id = self.get_or_create_location(
                        #     cursor, **self.get_location_fields(cols)
                        # )

                        event_id = self.create_event(
                            cursor,
                            **self.get_event_fields(cols),
                        )

                        tsunami_id = self.create_tsunami(
                            cursor,
                            event_id=event_id,
                            **self.get_tsunami_fields(cols),
                        )

                        self.create_damage(
                            cursor,
                            tsunami_id=tsunami_id,
                            **self.get_damage_fields(cols),
                        )

                    # if index == 10:
                    #     break
                    index += 1

                conn.commit()

    def create_table_location(self, cursor):
        cursor.execute("drop table if exists Location;")
        cursor.execute(
            """
        create table Location(
            id int identity(1,1) primary key,
            country nvarchar(250),
            location nvarchar(250)
        )
        """
        )

    def create_table_event(self, cursor):
        cursor.execute("drop table if exists Event")
        cursor.execute(
            """
        create table Event(
            id int identity(1,1) primary key,
            year int null,
            month int null,
            day int null,
            hour int null,
            minute int null,
            second int null,
            cause_code nvarchar(250),
            latitude float null,
            longitude float null,

            country nvarchar(250),
            location nvarchar(250),
        )
        """
        )

    def create_table_tsunami(self, cursor):
        cursor.execute("drop table if exists Tsunami;")
        cursor.execute(
            """
        create table Tsunami(
            id int identity(1,1) primary key,
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
            id int identity(1,1) primary key,
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

    def save_to_csv(self, result, file, headers=None):
        with open(file, "w", newline="") as csvfile:
            writer = csv.writer(csvfile)

            if headers:
                writer.writerow(headers)

            for item in result:
                writer.writerow(item)

    def query_1(self, cursor):
        return cursor.execute("select count(1) from Location;")

    def sql_1(sefl):
        return """
        select 'Event' as name, count(1) as count from Event
        union select 'Tsunami' as name, count(1) as count from Tsunami
        union select 'Damage' as name, count(1) as count from Damage;
        """

    def sql_2(sefl):
        return """
        SELECT  YEAR
            ,[1]
            ,[2]
            ,[3]
            ,[4]
            ,[5]
        FROM
        (
            -- source, quita times, para evitar conflijcots en pivo t
            SELECT  i.year
                ,i.country
                ,i.pos
            FROM
            (
                -- calcula las posicione s
                SELECT  e.year
                    ,e.country
                    ,COUNT(1)                                                              AS times
                    ,row_number() OVER (PARTITION BY e.year ORDER BY e.year,COUNT(1) DESC) AS pos
                FROM Event e --where e.year = 1804
                GROUP BY  e.year
                        ,e.country
            ) AS i
        ) AS t pivot (MAX(country) FOR pos IN ([1], [2], [3], [4], [5])) AS p
        """

    def sql_3(self):
        return """
        SELECT  country
            ,[1] AS year_1
            ,[2] AS year_2
            ,[3] AS year_3
            ,[4] AS year_4
            ,[5] AS year_5
        FROM
        (
            SELECT  i.country
                ,i.year
                ,i.pos
            FROM
            (
                SELECT  e.country
                    ,e.year
                    ,COUNT(1)                                                                    AS times
                    ,row_number() over (partition by e.country ORDER BY e.country,COUNT(1) desc) AS pos
                FROM Event e
                GROUP BY  e.country
                        ,e.year
            ) AS i
        ) AS t pivot ( MAX(year) for pos IN ([1], [2], [3], [4], [5]) ) AS p
        """

    def sql_4(self):
        return """
        SELECT  e.country
            ,AVG(d.total_damage) AS damage_avg
        FROM Damage d
        INNER JOIN Tsunami t
        ON t.id = d.tsunami_id
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.country
        ORDER BY e.country
        """

    def sql_5(self):
        return """
        SELECT  top 5 e.country
            ,SUM(d.total_deaths) AS total_deaths
        FROM Damage d
        INNER JOIN Tsunami t
        ON t.id = d.tsunami_id
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.country
        ORDER BY SUM(d.total_deaths) desc
        """

    def sql_6(self):
        return """
        SELECT  top 5 e.year
            ,SUM(d.total_deaths) AS total_deaths
        FROM Damage d
        INNER JOIN Tsunami t
        ON t.id = d.tsunami_id
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.year
        ORDER BY SUM(d.total_deaths) desc
        """

    def sql_7(self):
        return """
        SELECT  top 5 e.year
            ,COUNT(1) AS total_tsunamis
        FROM Event e
        GROUP BY  e.year
        ORDER BY COUNT(1) desc
        """

    def sql_8(self):
        return """
        SELECT  top 5 e.country
            ,SUM(d.total_houses_destroyed) AS total_houses_destroyed
        FROM Damage d
        INNER JOIN Tsunami t
        ON t.id = d.tsunami_id
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.country
        ORDER BY SUM(d.total_houses_destroyed) desc
        """

    def sql_9(self):
        return """
        SELECT  top 5 e.country
            ,SUM(d.total_houses_damaged) AS total_houses_damaged
        FROM Damage d
        INNER JOIN Tsunami t
        ON t.id = d.tsunami_id
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.country
        ORDER BY SUM(d.total_houses_damaged) desc
        """

    def sql_10(self):
        return """
        SELECT  e.country
            ,AVG(t.max_weater_height) AS avg_height
        FROM Tsunami t
        INNER JOIN Event e
        ON e.id = t.event_id
        GROUP BY  e.country
        ORDER BY AVG(t.max_weater_height) desc
        """

    def make_querys(self, number, output_dir="/workspace/results"):
        try:
            get_sql = getattr(
                self,
                "sql_{}".format(number),
            )
            filename = "{}/query_{}.csv".format(output_dir, number)
            with self.create_connection() as conn:
                df = pandas.read_sql_query(get_sql(), conn)
                df.to_csv(filename, index=False)
                # cursor = conn.cursor()

                # result = query(cursor)
                # headers = [item[0] for item in cursor.description]

                # if result:
                #     self.save_to_csv(
                #         result,
                #         "{}/query_{}.csv".format(output_dir, number),
                #         headers=headers,
                #     )

        except AttributeError as err:
            print(err.args)

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
        num = input("Ingrese el número de consulta (1 - 10):")
        handler.make_querys(num)
    else:
        print("OPción no reconocida")
