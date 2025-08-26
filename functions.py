#Este script está encargado de almacenar todas las funciones necesarias para el buen funcionamiento de los Script generadores de consolidados

import pandas as pd
from datetime import datetime
import os

#Funcion para leer los archivos de la COOISPI
def leer_archivo(ruta, hoja=None):
  try:
    if not os.path.exists(ruta):  #Validar si la ruta existe
      raise FileNotFoundError(f"La ruta '{ruta}' no existe.")

    if ruta.lower().endswith('.csv'):  #Si la ruta es un csv
      df = pd.read_csv(ruta)
      return df
    elif ruta.lower().endswith(('.xlsx', 'xls')):  #Sino, si es un xlsx o xls
      excel = pd.ExcelFile(ruta)

      if hoja not in excel.sheet_names:  #Verificar que la hoja solicitada exista
        raise ValueError(f"La hoja '{hoja}' no existe en el archivo. Hojas disponibles: {excel.sheet_names}")

      df = pd.read_excel(excel, sheet_name=hoja)  #Leer directamente la hoja
      return df
    else:
      raise ValueError("Formato no soportado. Solo se permiten archivos CSV y Excel.")
  except Exception as e:
    raise RuntimeError(f"Error al leer archivo: {e}")

def meses_en_espanol():
  return{
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

def Turnos():
  return{
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
  
# Paso 2: Crear una función de conversión segura
def convertir_a_float(valor):
  try:
    # Reemplazar ',' con '.' y eliminar separadores de millares
    return float(str(valor).replace('.', '').replace(',', '.'))
  except ValueError:
    return None  # Si no se puede convertir, devuelve None
  
def analizar_inconsistencias_categoricas(df, columnas_objetivo):
  resumen = {}

  for columna in columnas_objetivo:
    print(f"\n🧐 Análisis de la columna: {columna}\n{'-'*40}")

    # Valores únicos
    valores_unicos = df[columna].dropna().unique()
    print(f"Valores únicos ({len(valores_unicos)}): {valores_unicos}")

    # Faltantes
    nulos = df[columna].isnull().sum()
    print(f"🔸 Valores nulos: {nulos}")

    # Duplicidad con diferencias por espacios, mayúsculas o typos
    valores_limpios = df[columna].dropna().astype(str).str.strip().str.lower()
    conteo_limpios = valores_limpios.value_counts()
    print("\n🔸 Conteo de valores tras limpieza (minúsculas y espacios):")
    print(conteo_limpios)

    # Agregar al resumen
    resumen[columna] = {   #columna hace referencia a que se crean diccionarios dentro de otro, columna es la clave y lo demás es el valor en un diccionario que contiene claves como Tostador y Material
      "nulos": nulos,
      "valores_unicos": list(valores_unicos),
      "conteo_normalizado": conteo_limpios.to_dict()
    }

  return resumen

def renombrar_columnas(df):
  """
  Renombra las columnas de un DataFrame de Pandas a nombres más descriptivos.
  Args:
    df: El DataFrame de Pandas cuyas columnas se van a renombrar.
  Returns:
    Un nuevo DataFrame con las columnas renombradas.
  """

  nombres_nuevos = {
    'Tostador': 'Tostador ',
    'BacheControl':'Bache de Control',
    'DescripcionMaterial':'Descripción de Material',
    'Lote':'Lote  ',
    'DestinoReal':'Destino Real',
    'FechaHora':'Fecha Hora',
    'TiempoTotalTostion':'Tiempo Total  Tostion    (SEG )',
    'CafeVerde':'Cafe Verde (KG)',
    'CafeTostado':'Cafe Tostado     (KG)',
    'CafeTostadoRecal':'Cafe Tostado. Recal (KG)',
    'Merma':'Merma (%)',
    'TemperaturaCritica1':'Temperatura Critica  1\n(C °)',
    'TemperaturaCritica2':'Temperatura Critica 2      (C °)',
    'TemperaturaCritica3':'Temperatura Critica 3       (C °)',
    'TiempoCritico1':'Tiempo Critico  1 (Seg)',
    'TiempoCritico2':'Tiempo Critico 2 (Seg)',
    'TiempoCritico3':'Tiempo Critico 3\n(Seg)',
    'UltimaLlama':'Ultima LLama\n(C °)',
    'Agua':'Agua\n(L)',
    'Energia':'Energia\n(W)',
    'GasTostador':'Gas Tostador M3',
    'GasPostquemador':'Gas Postquemador M3 ',
    'TemperaturaCarga':'Temperatura Carga\n(°C)',
    'TiempoEntradaAgua':'Tiempo Entrada Agua (Seg)',
    'TiempoCalentamiento':'Tiempo Calentamiento (Seg)',
    'TiempoPrecalentamiento':'TiempoPrecalentamiento',
    'TiempoA':'Tiempo Arranque (Seg)',
    'GasTostadorA':'Gas Tostador Arranque (m3)',
    'GasPostQuemadorA':'Gas Postquemador Arranque (m3)',
    'EnergiaElectricaA':'Energía Electrica Arranque (kWh)',
    'TiempoCtmto':'Tiempo calentamiento (Seg)',
    'GasTostadorCtmto':'Gas Tostador Calentamiento (m3)',
    'GasPostquemadorCtmto':'Gas Postquemador Calentamiento (m3)',
    'EnergiaElectricaCtmto':'Energía Electrica Calentamiento (kWh)'
  }
  df_renombrado = df.rename(columns=nombres_nuevos)
  return df_renombrado

def desglozar_fecha_hora(df, columna_fecha_hora):
  if columna_fecha_hora not in df.columns:
    raise ValueError(f"La columna '{columna_fecha_hora}' no existe en el DataFrame.")
  df[columna_fecha_hora] = df[columna_fecha_hora].astype("datetime64[ns]")
  df['Año'] = df[columna_fecha_hora].dt.year
  df['Dia'] = df[columna_fecha_hora].dt.day_of_year
  df['Mes_Numero'] = df[columna_fecha_hora].dt.month
  df['Mes'] = df['Mes_Numero'].map(meses_en_espanol())
  df['Hora'] = df[columna_fecha_hora].dt.hour
  df['Turnos'] = df[columna_fecha_hora].dt.hour.apply(asignar_turno)
  df['Fecha'] = df[columna_fecha_hora].dt.strftime('%Y-%m-%d')
  return df

#Funcion para limpiar dataframe eliminando valores atípicos extremos en dos columnas (filtrado por rango intercuartílico)
def filtrado_IQR(df):

  col1 = "Indicador_m3/Ton_Tost"
  col2 = "Indicador_m3/Ton_Post"
  factor = 2.5  #permite ajustar el tamaño del filtro en este caso ampliandolo y permitiendo mas variabilidad

  # Límites para col1
  Q1_1 = df[col1].quantile(0.25)
  Q3_1 = df[col1].quantile(0.75)
  IQR_1 = Q3_1 - Q1_1
  lim_inf_1 = Q1_1 - 1.5 * IQR_1 * factor
  lim_sup_1 = Q3_1 + 1.5 * IQR_1 * factor

  # Límites para col2
  Q1_2 = df[col2].quantile(0.25)
  Q3_2 = df[col2].quantile(0.75)
  IQR_2 = Q3_2 - Q1_2
  lim_inf_2 = Q1_2 - 1.5 * IQR_2 * factor
  lim_sup_2 = Q3_2 + 1.5 * IQR_2 * factor

  # Aplicar filtro IQR en ambas columnas
  df_filtrado = df[
    (df[col1].between(lim_inf_1, lim_sup_1)) &
    (df[col2].between(lim_inf_2, lim_sup_2))
  ].copy()

  return df_filtrado

def resumen_descartes_por_tostador(df_original, df_filtrado, columna_tostador="Tostador "):
  """
  Compara el DataFrame original y el filtrado.
  Devuelve un resumen de cuántos datos fueron descartados por cada tostador,
  junto con su proporción (%).
  Parámetros:
  - df_original: DataFrame antes del filtrado.
  - df_filtrado: DataFrame después del filtrado.
  - columna_tostador: Nombre de la columna que identifica el tostador (por defecto: 'Tostador').
  Retorna:
  - resumen: DataFrame con conteo y porcentaje de descartes por tostador.
  - descartados: DataFrame con los registros descartados.
  """

  # Identificar los registros descartados
  descartados = df_original.loc[~df_original.index.isin(df_filtrado.index)]

  # Conteo y porcentaje por tostador
  conteo = descartados[columna_tostador].value_counts()
  porcentaje = descartados[columna_tostador].value_counts(normalize=True) * 100

  # Unir en un solo DataFrame
  resumen = pd.DataFrame({
    "Cantidad descartada": conteo,
    "Proporción (%)": porcentaje.round(2)
  })   #aqui tostador queda como indice necesitando un .reset_index()

  return resumen, descartados

def eliminar_duplicados_nan(df):
  #crear una "firma" única por fila, considerando todas las columnas (incluye NaN y fechas) ya que don .drop_duplicates() NaN != NaN
  df['_row_signature'] = df.apply(lambda row: tuple(row.values), axis=1)   #row.values devuelve un array con todos los valores de la fila y luego se transforman en una tupla

  #eliminar duplicados basandose en la columna firma manteniendo solo la primera aparición
  df_sin_duplicados = df.drop_duplicates(subset=['_row_signature']).copy()

  #eliminar la columna auxiliar
  df_sin_duplicados.drop(columns=['_row_signature'], inplace=True)

  return df_sin_duplicados

def conteo_baches(df):
  conteo_baches_perdidos = []

  # Agrupar por Fecha y Tostador
  #en fecha y tostador se guardan temporalmente esos datos y en grupo se guarda la fecha, tostador y las demas columnas con las filas correspondientes a dicho groupby
  for (fecha, tostador), grupo in df.groupby(['Fecha', 'Tostador ']):
      # Ordenar por Bache de Control para cada fecha y tostador
      grupo = grupo.sort_values(by='Bache de Control')
      # Identificar baches perdidos
      dif = grupo['Bache de Control'].diff().fillna(1)  # Diferencias entre valores de filas y NaN = 1
      # Contar el total de baches perdidos
      baches_perdidos = dif[dif > 1].sum() - dif[dif > 1].count()  #baches intermedios perdidos (ejm 104 - 100 = 3 - 1 = 3 --> entre 104 y 100 está 101, 102 y 103 = 3)
      # Agregar al resultado
      conteo_baches_perdidos.append({
          'Fecha': fecha,
          'Tostador ': tostador,
          '# Baches perdidos': int(baches_perdidos)
      })

  # Convertir la lista a un DataFrame
  df_conteo_baches_perdidos = pd.DataFrame(conteo_baches_perdidos)

  return df_conteo_baches_perdidos

def detectar_inicio_fecha(df, columna_fecha, columnas_objetivo):
  #ordenar el DataFrame por fecha, por si acaso no lo está
  df_ordenado = df.sort_values(columna_fecha).copy()

  #crear una máscara donde todas esas columnas están vacías
  solo_nulos = df_ordenado[columnas_objetivo].isna().all(axis=1) #Devuelve True solo si todas las columnas están vacías (NaN) en esa fila.

  #y otra máscara donde hay al menos un valor no nulo
  al_menos_un_valor = df_ordenado[columnas_objetivo].notna().any(axis=1) #Devuelve True si al menos una columna tiene valor (no es NaN) en esa fila

  # Buscar la primera fila donde ocurre la transición de solo_nulos -> al_menos_un_valor
  for i in range(len(df_ordenado) - 1):
      if solo_nulos.iloc[i] and al_menos_un_valor.iloc[i + 1]:
          fecha_cambio = df_ordenado.iloc[i + 1][columna_fecha]
          print(f"🟢 Primera aparición de datos en esas columnas fue en: {fecha_cambio}")
          break
  else:
      print("No se encontró una transición de NaN a valores en esas columnas.")
  return None

def rango_valores_atipicos(df, min_gas_post_bache, max_gas_post_bache, columna):
  df_at_postq = df[~df[columna].between(min_gas_post_bache, max_gas_post_bache)]  #se filtran los valores atipicos
  return df_at_postq

def reemplazar_atipicos_prom(df, min_gas_post_bache, max_gas_post_bache, columna):
  #identificar los datos válidos y no válidos
  validos = df[columna].between(min_gas_post_bache, max_gas_post_bache)  #se guarda una serie de booleanos con True para los datos validos

  #calcular el promedio de los valores válidos
  promedio_validos = df.loc[validos, columna].mean()

  #reemplazar los valores no válidos(los que estan fuera del rango) por el promedio
  df.loc[~validos, columna] = promedio_validos
  return df

def consumo_gas_tostador(df):

  tostadores = [1, 2, 3, 4, 5, 6, 7]
  conteos = {}
  dataframes_tostadores = []

  for i in tostadores:
    #Columnas sin variaciones
    columnas = ['PERIODO ','AÑO','MES ','DIA ']

    #columnas variables
    if i == 7:
      columnas += ['TOSTADOR 7 (lilla)','CAFÉ VERDE 7']
    else:
      columnas += [f'TOSTADOR {i}', f'POST-QUEMADOR {i if i != 4 else "4A"}', f'CAFÉ VERDE {i}']

    df_Tostador_amb = df.loc[:,columnas].copy()  #se toman las filas/registros pertenecientes a cada Tostador

    if i == 7:
      rename = {
          'PERIODO ': 'Fecha',
          'AÑO': 'Año',
          'MES ': 'Mes',
          'DIA ': 'Dia',
          'TOSTADOR 7 (lilla)': 'Consumo gas tostador',
          'CAFÉ VERDE 7': 'Café verde'
      }
    else:
      rename = {
          'PERIODO ': 'Fecha',
          'AÑO': 'Año',
          'MES ': 'Mes',
          'DIA ': 'Dia',
          f'TOSTADOR {i}': 'Consumo gas tostador',
          f'POST-QUEMADOR {i if i != 4 else "4A"}': 'Consumo gas postquemador',
          f'CAFÉ VERDE {i}': 'Café verde'
      }

    df_Tostador_amb.rename(columns=rename, inplace=True)  #renombrar columnas

    df_Tostador_amb.dropna(subset=['Consumo gas tostador','Café verde'], inplace=True)  #eliminar filas con datos de tipo NaN en Consumo gas tostador y/o Café verde

    df_Tostador_amb['Tostador'] = f'TOSTADOR {i}'

    if i == 7:
      df_Tostador_amb['Consumo gas postquemador'] = 0

    dataframes_tostadores.append(df_Tostador_amb)  #se guarda cada dataframe en una posicion de la lista

    conteos[f'TOSTADOR {i}'] = df_Tostador_amb.shape[0]  #diccionario de visualizacion para ver por tostador cuantas filas/registros contiene


  df_Total_amb = pd.concat(dataframes_tostadores, axis=0, ignore_index=True)  #unir los dataframes almacenador en la lista dataframes_tostadores uno debajo del otro

  df_Total_amb['Ciudad'] = "Medellín"

  df_Total_amb['Mes'] = df_Total_amb['Mes'].str.capitalize()

  return df_Total_amb, conteos

def consumo_gas_secador(df):

  secadores = [2, 3, 4]
  conteos = {}
  dataframes_secadores = []

  for i in secadores:
    #Columnas sin variaciones
    columnas = ['PERIODO ','AÑO','MES ','DIA ', f'SECADOR {i}', f'CAFÉ SECADOR {i}']

    df_Secador_amb = df.loc[:,columnas].copy()  #se toman las filas/registros pertenecientes a cada Secador

    rename = {
        'PERIODO ': 'Fecha',
        'AÑO': 'Año',
        'MES ': 'Mes',
        'DIA ': 'Dia',
        f'SECADOR {i}': 'Consumo gas secador',
        f'CAFÉ SECADOR {i}': 'Café verde'
    }

    df_Secador_amb.rename(columns=rename, inplace=True)  #renombrar columnas

    df_Secador_amb.dropna(subset=['Consumo gas secador','Café verde'], inplace=True)  #eliminar filas con datos de tipo NaN en Consumo gas secador y/o Café verde

    df_Secador_amb['Secador'] = f'SECADOR {i}'

    dataframes_secadores.append(df_Secador_amb)  #se guarda cada dataframe en una posicion de la lista

    conteos[f'SECADOR {i}'] = df_Secador_amb.shape[0]  #diccionario de visualizacion para ver por secador cuantas filas/registros contiene


  df_Total_amb_Secado = pd.concat(dataframes_secadores, axis=0, ignore_index=True)  #unir los dataframes almacenador en la lista dataframes_secadores uno debajo del otro

  df_Total_amb_Secado['Ciudad'] = "Medellín"

  df_Total_amb_Secado['Mes'] = df_Total_amb_Secado['Mes'].str.capitalize()

  return df_Total_amb_Secado, conteos

def consumo_gas_aglomerado(df):

  aglomerados = [1, 2]
  conteos = {}
  dataframes_aglomerado = []

  for i in aglomerados:
    #Columnas sin variaciones
    columnas = ['PERIODO ','AÑO','MES ','DIA ', f'AGLOMERADOR {i}', f'CAFÉ AGLOMERADOR {i}']

    df_Aglomerador_amb = df.loc[:,columnas].copy()  #se toman las filas/registros pertenecientes a cada Aglomerador

    rename = {
        'PERIODO ': 'Fecha',
        'AÑO': 'Año',
        'MES ': 'Mes',
        'DIA ': 'Dia',
        f'AGLOMERADOR {i}': 'Consumo gas Aglomerador',
        f'CAFÉ AGLOMERADOR {i}': 'Café verde'
    }

    df_Aglomerador_amb.rename(columns=rename, inplace=True)  #renombrar columnas

    df_Aglomerador_amb.dropna(subset=['Consumo gas Aglomerador','Café verde'], inplace=True)  #eliminar filas con datos de tipo NaN en Consumo gas aglomerador y/o Café verde

    df_Aglomerador_amb['Aglomerador'] = f'AGLOMERADOR {i}'

    dataframes_aglomerado.append(df_Aglomerador_amb)  #se guarda cada dataframe en una posicion de la lista

    conteos[f'AGLOMERADOR {i}'] = df_Aglomerador_amb.shape[0]  #diccionario de visualizacion para ver por secador cuantas filas/registros contiene


  df_Total_amb_Aglomerado = pd.concat(dataframes_aglomerado, axis=0, ignore_index=True)  #unir los dataframes almacenador en la lista dataframes_aglomerado uno debajo del otro

  df_Total_amb_Aglomerado['Ciudad'] = "Medellín"

  df_Total_amb_Aglomerado['Mes'] = df_Total_amb_Aglomerado['Mes'].str.capitalize()

  return df_Total_amb_Aglomerado, conteos

def convertir_datos(df, columnas_convertir):
  #Aplicar la conversión segura y sobrescribir las columnas originales
  for columna in columnas_convertir:
    df[columna] = df[columna].apply(convertir_a_float)  #devuelve un np.nan si el dato no es convertible

  #Identificar y aislar filas con valores no convertibles
  df_no_convertibles = df[df[columnas_convertir].isna().any(axis=1)]   #guarda las filas problematicas donde al menos una conversion en el registro fallo
  df_convertidos = df.dropna(subset=columnas_convertir)    #filtra los registros donde todas las columnas se pudieron convertir (no tenian NaN)

  return df_no_convertibles, df_convertidos, df

def calcular_indicador(df, tipo_proceso, columna_consumo_gas, columna_postquemador=None):
  df.drop(df.loc[df['Café verde'] == 0].index,inplace=True)

  indicadores = {
    'tostador': lambda df: df.assign(**{
      "Indicador_m3/Ton_Tost" : df[columna_consumo_gas] / df['Café verde'],    #se calcula el consumo de gas por cada tonelada de cafe
      "Indicador_m3/Ton_Post" : df[columna_postquemador] / df['Café verde']    #se calcula el consumo de gas por cada tonelada de cafe en el portquemador
    }),
    'secado': lambda df: df.assign(**{
      "Indicador_m3/Ton_Secado" : df[columna_consumo_gas] / df['Café verde']
    }),
    'aglomerado': lambda df: df.assign(**{
      "Indicador_m3/Ton_Aglo" : df[columna_consumo_gas] / df['Café verde']
    })
  }

  df = indicadores[tipo_proceso](df)

  return df

def generar_fechas(fecha_inicio):
  # Definir la fecha inicial y la fecha final
  start_date = pd.to_datetime(fecha_inicio) # Definición del inicio de fechas
  end_date = datetime.today()

  # Generar un rango de fechas desde el 2024 hasta la fecha actual
  date_range = pd.date_range(start=start_date, end=end_date, freq='D')

  # Crear y asignar el DataFrame con el formato "día/mes/año"
  df_Fechas = pd.DataFrame({
      'Fecha': date_range.strftime('%d/%m/%Y')  # Cambiar el formato a "día/mes/año"
  })

  return df_Fechas
