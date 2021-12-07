import pyodbc
import os

CONN_STR = "DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};"


DB_HOST = os.environ.get("DB_HOST")
DB_NAME = os.environ.get("DB_NAME")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")


class EtlHandler:
    def get_conn_str(self):
        return CONN_STR.format(server="db", database="tempdb", username="sa", password=DB_PASSWORD, )

    def create_connection(self):
        return pyodbc.connect(self.get_conn_str())


handler = EtlHandler()

print(handler.get_conn_str())
print(handler.create_connection())

running = True
while running:
    print("""
        MENU
        -----------------------------
        1. Crear modelo
        2. Crear información
        3. Realizar consultas
        4. Salir
        -----------------------------
    """)
    option = input("Selecciona una opción:")
    if option == "4":
        running = False
    else:
        print("OPción no reconocida")
