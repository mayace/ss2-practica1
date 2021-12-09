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
        location_id=None,
        register_at=None,
        latitude=None,
        longitude=None,
        cause_code=None,
    ):
        sql = """
            insert into Event(register_at, location_id, latitude, longitude, cause_code)
            output inserted.id
            values (?, ?, ?, ?, ?);
        """

        result = cursor.execute(
            sql, (register_at, location_id, latitude, longitude, cause_code)
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
        register_at = None

        try:
            register_at = datetime.datetime(
                self.parse_int(cols.year),
                self.parse_int(cols.month) or 1,
                self.parse_int(cols.day) or 1,
                self.parse_int(cols.hour),
                self.parse_int(cols.minute),
                self.parse_int(cols.second),
            )
        except ValueError as err:
            print(err.args, cols)
            # raise

        d = {}

        d.setdefault("register_at", register_at)
        d.setdefault("cause_code", self.get_str_field(cols.cause_code))
        d.setdefault("latitude", self.parse_float(cols.latitude))
        d.setdefault("longitude", self.parse_float(cols.longitude))

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

                        location_id = self.get_or_create_location(
                            cursor, **self.get_location_fields(cols)
                        )

                        event_id = self.create_event(
                            cursor,
                            location_id=location_id,
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
            country text,
            location text
        )
        """
        )

    def create_table_event(self, cursor):
        cursor.execute("drop table if exists Event")
        cursor.execute(
            """
        create table Event(
            id int identity(1,1) primary key,
            register_at datetime,
            cause_code text,
            latitude float null,
            longitude float null,

            location_id int,
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

    def make_querys(self, number, output_dir="/workspace/results"):
        try:
            query = getattr(
                self,
                "query_{}".format(number),
            )
            filename = "{}/query_{}.csv".format(output_dir, number)
            with self.create_connection() as conn:
                df = pandas.read_sql_query("select 1 as cuatro, count(1) as count from Location;", conn)
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
