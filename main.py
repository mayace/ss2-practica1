import os, pyodbc, csv

CONN_STR = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"


DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")


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

    def create_information(self, pathfile):
        with open(pathfile, mode="r") as file:
            for item in csv.reader(file):
                print(item)
                break

    def create_models(
        self,
    ):
        print("done")

    def make_querys(
        self,
    ):
        print("done")


handler = EtlHandler()

print(handler.get_conn_str())
print(handler.create_connection())

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
