import pyodbc

def conectar_sql(driver, server, database, username, password):
    conn_str = (f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}')  #tipo de base de datos, direccion del servidor, nombre de la BD, usuario, contraseña
    return pyodbc.connect(conn_str)