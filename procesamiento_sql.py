import pandas as pd
from datetime import datetime
import pyodbc
from conection_sql import conectar_sql
from consultas_sql import consulta_tostadores_2025, consulta_consumo_MES

meses_en_espanol = {
    1: 'Enero',
    2: 'Febrero',
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}

Turnos ={
    1: 'Turno I (6am -2pm)',
    2: 'Turno II (2pm -10pm)',
    3: 'Turno III (10pm -6am)'
}

def asignar_turno(hora):
    if 6 <= hora< 14:
        return 'Turno 1 (6am -2pm)'
    elif 14 <= hora< 22:
        return 'Turno II (2pm -10pm)'
    else:
        return 'Turno III (10pm -6am)'
    
def ejecutar_consulta(query, server, database, username, password):
    try:
        conn = conectar_sql('{SQL Server}', server, database, username, password)  #se hace conexion activa a la bd
        df = pd.read_sql(query, conn)   #ejecutar la consulta y almacenar los resultados en un DataFrame(pd.read_sql() --> ejecuta consulta y entrega tabla, query --> consulta sql, conn --> conexion a la BD)
        conn.close()   #cerrar conexion
        return df
    except pyodbc.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return pd.DataFrame()
    
def preparar_dataframe(df):
    df = df.copy(deep=True)
    df['Fecha'] = df['Fecha'].astype("datetime64[ns]")
    df['Mes_T'] = df['Mes'].map(meses_en_espanol)
    df['Turnos'] = df['Fecha'].dt.hour.apply(asignar_turno)
    return df


pd.options.display.float_format='{:,.3f}'.format  #muestre numeros decimales (floats), los formatee con 3 decimales y coma como separador de miles

server = '172.28.36.36'    #dirección IP o nombre del servidor que contiene la base de datos
database = 'eMesOS3'       #nombre de la base de datos a la que quieres conectarte
username = 'BI'            #usuario con permisos para acceder a esa base de datos
password = 'C0lc4f32023'   #contraseña del usuario

fecha_inicio = '2025-01-01'
fecha_fin = datetime.now()
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")
#fecha_fin_str = "2024-04-02"      # Si necesito hacer la consulta hasta una fecha en especifico

query = consulta_tostadores_2025(fecha_inicio, fecha_fin_str)
df_new_read = ejecutar_consulta(query, server, database, username, password)

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
Ruta_Excel_2025_IC = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Detalle_TostionMDE_2025_MES.xlsx'                  #RUTA IC para 2025 excel
Ruta_csv_2025_IC = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Detalle_TostionMDE_2025_MES.csv'                     #RUTA IC para 2025 csv
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
df_new_read.to_excel(Ruta_Excel_2025_IC, index=False)
df_new_read.to_csv(Ruta_csv_2025_IC, index=False)

Ruta_csv_2023_2024 = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Detalle_TostionMDE_2023_2024_MES.csv'               #RUTA IC que contiene la información anterior (año 2023-2024)

df_ant = pd.read_csv(Ruta_csv_2023_2024)
# Asegurarte de que df1 tenga las mismas columnas que df2
df_ant_aligned = df_ant.reindex(columns=df_new_read.columns)  #asegura que df_ant tenga exactamente las mismas columnas y en el mismo orden que df_new_read
# Concatenar ambos dataframes
df_total = pd.concat([df_ant_aligned, df_new_read], ignore_index=True)  #cancatena por defecto verticalmente axis=0 y restablece los indices
index = pd.Index(range(1, len(df_total) + 1)) #los indices de df_total se restablecen desde 1
df_total.index = index

#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
df_total.to_csv(r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Detalle_TostionMDE_2023_2024_2025_MES.csv')              #RUTA IC con 2023-2024 y 2025
#----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

fecha_inicio = '2024-01-01'
fecha_fin = datetime.now()
fecha_fin_str = fecha_fin.strftime("%Y-%m-%d")
#fecha_fin_str="2024-04-02"      # Si necesito hacer la consulta hasta una fecha en especifico

query = consulta_consumo_MES(fecha_inicio, fecha_fin_str)
df_new_read = ejecutar_consulta(query, server, database, username, password)

df = preparar_dataframe(df_new_read)

#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
df.to_excel(r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Consumos_MES.xlsx', sheet_name='Hoja1', index=False)         #RUTA IC
#-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

# print(df_new_read.shape)
# print(df_total.shape)
# print(df.shape)