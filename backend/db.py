import pyodbc

def get_db_connection():
    conn_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        "SERVER=localhost,1433;"           # host,puerto
        "DATABASE=DemoMonitoring;"          # <-- ojo al nombre; pon el correcto
        "UID=sa;"
        "PWD=Snorlax14;"
        "Encrypt=no;"                       # o Encrypt=yes;TrustServerCertificate=yes
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str, timeout=5)
