import pyodbc

def get_db_connection():
    conn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};'
        'SERVER=localhost;'
        'DATABASE=nombre_base_datos;'
        'UID=usuario;'
        'PWD=contraseña'
    )
    return conn
