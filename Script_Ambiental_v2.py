import pandas as pd
import os
import functions as fc
import importlib
importlib.reload(fc)

##**1. Carga de Datos**

#**Variables globales de las rutas necesarias**
#Lectura del archivo consolidado (años 2023-2024-2025) se realiza luego del script que accede a las bases de datos de SQL (este Script no se encuentra en Google Colab) y lectura de Consumos de gas consolidados por MES

# df_Tostion_Mes = r'G:\.shortcut-targets-by-id\11pRq2eSspqJd86txF6wc2_q362MdeLLp\Ciclo_Mejora_Tostion\Datos\Historicos_MES\Detalle_TostionMDE_2023_2024_2025_MES.csv' #ruta original
df_Tostion_Mes = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Detalle_TostionMDE_2023_2024_2025_MES.csv'

# df_Tostion_MES_totales = r'G:\.shortcut-targets-by-id\11pRq2eSspqJd86txF6wc2_q362MdeLLp\Ciclo_Mejora_Tostion\Datos\Historicos_MES\Consumos_MES.xlsx' #ruta original
df_Tostion_MES_totales = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Historicos_MES\Consumos_MES.xlsx'

# df_Ambiental = r'G:\.shortcut-targets-by-id\11pRq2eSspqJd86txF6wc2_q362MdeLLp\Ciclo_Mejora_Tostion\Datos\Ambiental\Consumo_Gas_Proceso.csv' #ruta original
df_Ambiental = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Ambiental\Consumo_Gas_Proceso.csv'


RUTA_BASE = r'\\10.28.5.232\s3-1colcafeci-servicios-jtc\TPM\Colcafé Formularios\BD Ind Tostion\Datos_Power_BI\\'


#Validacion de existencia de las rutas
for nombre, ruta in {
    "df_Tostion_Mes": df_Tostion_Mes,
    "df_Tostion_MES_totales": df_Tostion_MES_totales,
    "df_Ambiental": df_Ambiental,
    "RUTA_BASE": RUTA_BASE,
}.items():
  if not os.path.exists(ruta):
    print(f"La ruta '{nombre}' no fue encontrada.")
  else:
    print(f"Ruta '{nombre}' encontrada.")

df_Tostion_Mes = pd.read_csv(df_Tostion_Mes, sep=',')
df_Tostion_MES_totales = fc.leer_archivo(df_Tostion_MES_totales, hoja="Hoja1")
df_Ambiental = fc.leer_archivo(df_Ambiental)

num_filas = df_Tostion_Mes.shape[0]
num_columnas = df_Tostion_Mes.shape[1]

print(f"La cantidad de filas del conjunto de datos es:{num_filas} y la cantidad de columnas es: {num_columnas}")
# df_Tostion_Mes.head(3)

df = df_Tostion_Mes.copy(deep=True)

df.shape

#Renombre de las columnas
df = fc.renombrar_columnas(df)
# df.head()

columnas_a_excluir = ['TiempoPrecalentamiento','Tiempo Arranque (Seg)','Gas Tostador Arranque (m3)', 'Gas Postquemador Arranque (m3)','Energía Electrica Arranque (kWh)',
                      'Tiempo calentamiento (Seg)','Gas Tostador Calentamiento (m3)','Gas Postquemador Calentamiento (m3)','Energía Electrica Calentamiento (kWh)']

df_filtrado = df.drop(columns=columnas_a_excluir)   #eliminar columnas a excluir del dataframe

mas_de_un_nulo = df_filtrado.isnull().sum(axis=1) >= 1  #identificar filas con valores nulos iguales o mayores a 1 campo

df_resultado = df[mas_de_un_nulo]  #usar esa máscara para filtrar el df original


df_resultado.head(5)
print(f"La cantidad de filas nulas es : {df_resultado.shape[0]}")

#filtrar el dataframe con los registros que no tienen valores nulos
df = df[~mas_de_un_nulo].copy()

#eliminar la Columna de "Unnamed: 0"
df.drop(columns='Unnamed: 0', inplace=True)
df.head(5)

#agrega columnas de Año, Día, Mes, Hora, Turno y Fecha basadas en 'Fecha Hora'
df = fc.desglozar_fecha_hora(df, 'Fecha Hora')

#**Validación y eliminación de filas repetidas**

#eliminar filas/registros duplicados con ayuda de una firma y datos en tuplas para tomar los NaN como datos iguales cosa qu el .drop_duplicates no logra
df_sin_duplicados = fc.eliminar_duplicados_nan(df)

#mostrar el nuevo tamaño del DataFrame
print(f"Filas originales: {df.shape[0]}")
print(f"Filas después de eliminar duplicados: {df_sin_duplicados.shape[0]}, se eliminaron {df.shape[0]-df_sin_duplicados.shape[0]} filas")

#asignar el nuevo dataframe
df = df_sin_duplicados.copy()

#**Conteo de baches perdidos**

#contar los saltos de registros de datos para cada tostador
df_conteo_baches_perdidos = fc.conteo_baches(df)

#columnas de interés y que la columna de fecha esté en datetime
columnas_objetivo = ['TiempoPrecalentamiento','Tiempo Arranque (Seg)','Gas Tostador Arranque (m3)', 'Gas Postquemador Arranque (m3)','Energía Electrica Arranque (kWh)',
                      'Tiempo calentamiento (Seg)','Gas Tostador Calentamiento (m3)','Gas Postquemador Calentamiento (m3)','Energía Electrica Calentamiento (kWh)']

#detectar la primera aparición de datos en x columnas
fc.detectar_inicio_fecha(df, 'Fecha Hora', columnas_objetivo)

#Filtrar desde la Fecha Hora donde comienzan a darse datos reales e identificar filas con valores nulos iguales o mayores a 1 campo para validaciones
#se hace solamente para validar
df_filtrado = df.loc[df['Fecha Hora']>="2025-01-02"]

mas_de_un_nulo = df_filtrado.isnull().sum(axis=1) >= 1  #Genera un dataframe de booleanos, si la celda está vacia True(1) y si tiene datos  False(0)

df_resultado = df_filtrado[mas_de_un_nulo] #Usar esa máscara para filtrar en el df filtrado

# df_resultado.head(5)

#se establece un rango para filtrar solo datos "atipicos" fuera de un rango de [0. 130]
min_gas_post_bache = 0
max_gas_post_bache = 130
df_at_postq = fc.rango_valores_atipicos(df, min_gas_post_bache, max_gas_post_bache, 'Gas Postquemador M3 ')

#Verificaremos a que tostadores corresponden dichos valores atipicos o fuera del rango y su proporción en base a todos los datos
conteo = df_at_postq['Tostador '].value_counts()
porcentaje = df_at_postq['Tostador '].value_counts(normalize=True) * 100
resultado = pd.DataFrame({'Cantidad': conteo, 'Proporción (%)': porcentaje.round(2)})
resultado

#Como se había comentado y tenían sospechas, la mayoria de los consumos de gas por bache mayor a los 100 m3 corresponden al postquemador del Tostador 4. Con la finalidad de no eliminar estos datos, se procederá a rellenar estos datos atípicos con el promedio del consumo de gas por bache de los datos sin incluirlos.

#guardar una copia del original
df_original = df.copy()

#reemplazar los valores atípicos por el promedio de los valores válidos
df = fc.reemplazar_atipicos_prom(df, min_gas_post_bache, max_gas_post_bache, 'Gas Postquemador M3 ')

#**Modificación Manual sobre el consumo de gas del tostador 7**
#Se multiplicó el valor del consumo de gas por el 25%

df.loc[df['Tostador '] == "TOSTADOR 7"]['Gas Tostador M3']

#se esta multiplicando por 1.0 para futuras transformaciones y a su vez validar el buen funcionamiento de la mascara
mask = (df['Tostador '] == 'TOSTADOR 7') & (df['Fecha Hora'] < pd.Timestamp('2025-06-09'))
df.loc[mask, 'Gas Tostador M3'] *= 1.0

df.loc[df['Tostador '] == "TOSTADOR 7"]['Gas Tostador M3']

#**Operaciones con los datos sin realizar filtros**
df_sn_Filtros = df.copy(deep=True)

# Columnas_Totalizados = ["Fecha","Año","Mes","Dia","'Tostador ","Gas Postquemador M3 ","Gas Tostador M3","Cafe Verde (KG)","Indicador_m3/Ton_Tost","Indicador_m3/Ton_Tost","Indicador_m3/Ton_Total",
#                       "Tiempo Arranque (Seg)","Gas Tostador Arranque (m3)","Gas Postquemador Arranque (m3)","Energía Electrica Arranque (kWh)","Tiempo calentamiento (Seg)","Gas Tostador Calentamiento (m3)",
#                       "Gas Postquemador Calentamiento (m3)","Energía Electrica Calentamiento (kWh)"]

columnas_totalizados = ["Cafe Verde (KG)", "Gas Postquemador M3 ", "Gas Tostador M3", "Tiempo Arranque (Seg)", "Gas Tostador Arranque (m3)",
                        "Gas Postquemador Arranque (m3)","Energía Electrica Arranque (kWh)", "Tiempo calentamiento (Seg)", "Gas Tostador Calentamiento (m3)",
                        "Energía Electrica Calentamiento (kWh)"]

#se genera un dataframe agrupado por Fecha y Tostador sumando sus columnas numericas
agg_dict = {col: "sum" for col in columnas_totalizados}
df_Tostadores_Totalizados = df_sn_Filtros.groupby(['Fecha','Tostador ']).agg(agg_dict).reset_index()

#**Indicador de consumo de gas por tonelada producida de café verde:** ahora, teniendo unos datos más organizados y filtrados, agregaremos dos columnas más al dataframe asociadas al indicador de consumo de gas por tonelada de café verde procesado; este indicador se calcula dividiendo el consumo de gas sobre las toneladas de café verde. Estas dos columnas serán objeto de un análisis descriptivo mas profundo en las siguiente sección, ya que indican que tan eficiente son los tostadores.

#generacion de indicadores de eficiencia energética e hídrica estandarizados por tonelada de cafe verde procesado
#para verificar que tan eficiente es cada tostador, validar variaciones anormales y comparar el rendimiento entre fechas, turnos o maquinas

df['Indicador_kWh/Ton'] = (df['Energia\n(W)'] / df['Cafe Verde (KG)']) * 1000                # Generación del Indicador kWh/Ton (Indicador de energía vs café verde)
df['Indicador_m3/Ton_Tost'] = (df['Gas Tostador M3'] / df['Cafe Verde (KG)']) * 1000         # Generación del Indicador m3/Ton-tostador
df['Indicador_m3/Ton_Post'] = (df['Gas Postquemador M3 '] / df['Cafe Verde (KG)']) * 1000    # Generación del Indicador m3/Ton-Postquemador
df['Indicador_m3/Ton_Total'] = df['Indicador_m3/Ton_Post'] + df['Indicador_m3/Ton_Tost']     # Generación del Indicador m3/Ton Totalizado (Postquemador + Tostador)
df['Indicador_Lts/Ton'] = (df['Agua\n(L)'] / df['Cafe Verde (KG)']) * 1000                   # Generación del Indicador Lts/Ton (Indicador de agua vs café verde)

#**Diagrama de Cajas y Bigotes**
#Nos basaremos en un diagrama de Cajas para analizar la distribución de los datos asociados al indicador de Consumo de gas por tonelada de Café producido para Tostador y Postquemador con la intención de visualizar la mediana, los cuartiles y los posibles valores atipicos.

# --- Visualización de Boxplots para validación de datos ---
# fig, axs = plt.subplots(1, 2, figsize=(14, 5))                                  #se crea una figura (fig) con 1 fila y 2 columnas (axs es una lista de ejes: axs[0] y axs[1] y tamaño (figsize=(alto, ancho)))
# # Boxplot del Indicador de Gas en Tostador
# sns.boxplot(data=df, y='Indicador_m3/Ton_Tost', ax=axs[0], color='skyblue')     #.bloxplot (grafico de cajas y bigotes), data(dataframe a usar), y='columna'(columna a graficar), ax=axs[0](primer recuerdo/eje), color(color de grafico)
# axs[0].set_title('Boxplot - Indicador de gas/Ton en Tostador')                  #titulo principal
# axs[0].set_ylabel('m3/Ton Café Verde')                                          #titulo eje y para cambiar el por defecto
# axs[0].grid(True)                                                               #.grid(True)(mostrar cuadricula)

# # Boxplot del Indicador de Gas en Postquemador
# sns.boxplot(data=df, y='Indicador_m3/Ton_Post', ax=axs[1], color='salmon')      #.bloxplot (grafico de cajas y bigotes), data(dataframe a usar), y='columna'(columna a graficar), ax=axs[2](segundo recuerdo/eje), color(color de grafico)
# axs[1].set_title('Boxplot - Indicador de gas/Ton en Postquemador')              #titulo principal
# axs[1].set_ylabel('m3/Ton Café Verde')                                          #titulo eje y para cambiar el por defecto
# axs[1].grid(True)                                                               #.grid(True)(mostrar cuadricula)

# plt.tight_layout()                                                              #ajusta automaticamente el espaciado entre los subplots
# # plt.show()                                                                      #muestra el grafico

#Nos soportaremos tambíen en el siguiente dataframe para sacar conclusiones iniciales respecto a los Indicadores de consumo de gas por tonelada de café producidos en Tostador y Postquemador.

# Estadisticas_Indicadores = df[['Indicador_m3/Ton_Tost', 'Indicador_m3/Ton_Post']].describe()
# Estadisticas_Indicadores

df_filtrado = fc.filtrado_IQR(df)
resumen, descartados = fc.resumen_descartes_por_tostador(df, df_filtrado)
# resumen.head()

resumen = resumen.reset_index()                                          #restaura los indices conservando el anterior como columna en este caso 'Tostador'
resumen.columns = ["Tostador", "Cantidad descartada", "Proporción (%)"]  #se renombran los nombres de las columnas, sin rename sino que es sobrescribiendo ya que no esta cambiando de antiguos a nuevos isno validando y definiendo un orden
resumen=resumen.sort_values(by="Cantidad descartada", ascending=False)   #se ordena la informacion de forma descendiente (mayor a menor)
resumen

# Con base en la anterior información, la cantidad de registros atipicos representan...
Atipicos = resumen['Cantidad descartada'].sum()
Atipicos_porcent = (resumen['Cantidad descartada'].sum() / df.shape[0]) * 100
print(f"Con base en la anterior información, la cantidad de registros descartados son:{Atipicos} lo cual representan el {Atipicos_porcent:.2f}% de los datos")

#se crea una copia del dataframe filtrado
df = df_filtrado.copy()

# fig, axs = plt.subplots(1, 2, figsize=(14, 5))
# # Boxplot del Indicador de Gas en Tostador
# sns.boxplot(data=df, y='Indicador_m3/Ton_Tost', ax=axs[0], color='skyblue')
# axs[0].set_title('Boxplot - Indicador de gas/Ton en Tostador')
# axs[0].set_ylabel('m3/Ton Café Verde')
# axs[0].grid(True)

# # Boxplot del Indicador de Gas en Postquemador
# sns.boxplot(data=df, y='Indicador_m3/Ton_Post', ax=axs[1], color='salmon')
# axs[1].set_title('Boxplot - Indicador de gas/Ton en Postquemador')
# axs[1].set_ylabel('m3/Ton Café Verde')
# axs[1].grid(True)

# plt.tight_layout()
# # plt.show()

columnas = ["Tostador ", "Material"]
informe = fc.analizar_inconsistencias_categoricas(df, columnas)  #es un diccionario de diccionarios

#**Listado de Tostadores y Descripción de Materiales por Tostador**

combinaciones_unicas = df[['Tostador ', 'Descripción de Material']].drop_duplicates().reset_index(drop=True)
# combinaciones_unicas.head(4)

###**Tratamiento de los datos de consumo de gas desde ambiental**
#**Proceso de extraccion de los datos para Tostador**

df_Total_amb, conteos = fc.consumo_gas_tostador(df_Ambiental)

df_Total_amb.loc[(df_Total_amb['Mes']=="Junio") & (df_Total_amb['Año']==2025) ]

columnas_convertir = ['Consumo gas tostador', 'Consumo gas postquemador', 'Café verde']
df_no_convertibles, df_convertidos, df_Total_amb = fc.convertir_datos(df_Total_amb, columnas_convertir)

df_Tostadores_Totalizados_amb = df_Total_amb.groupby(['Fecha','Tostador']).agg({"Café verde": 'sum', "Consumo gas postquemador": 'sum',"Consumo gas tostador":'sum'}).reset_index()

df_Total_amb = fc.calcular_indicador(df_Total_amb, 'tostador', 'Consumo gas tostador', 'Consumo gas postquemador')

#**Proceso de extraccion de los datos para Secado**

df_Total_amb_Secado, conteos = fc.consumo_gas_secador(df_Ambiental)

columnas_convertir = ['Consumo gas secador','Café verde']
df_no_convertibles, df_convertidos, df_Total_amb_Secado = fc.convertir_datos(df_Total_amb_Secado, columnas_convertir)

df_Total_amb_Secado = fc.calcular_indicador(df_Total_amb_Secado, 'secado', 'Consumo gas secador')

#**Proceso de extraccion de los datos para Aglomerado**

df_Total_amb_Aglomerado, conteos = fc.consumo_gas_aglomerado(df_Ambiental)

columnas_convertir = ['Consumo gas Aglomerador','Café verde']
df_no_convertibles, df_convertidos, df_Total_amb_Aglomerado = fc.convertir_datos(df_Total_amb_Aglomerado, columnas_convertir)

df_Total_amb_Aglomerado = fc.calcular_indicador(df_Total_amb_Aglomerado, 'aglomerado', 'Consumo gas Aglomerador')

#**Calculo de los totalizados por dia**

# Columnas_Totalizados=["Fecha","Año","Mes","Dia","Tostador ","Consumo gas tostador","Consumo gas postquemador","Café verde"]
# df_Tostadores_Totalizados_amb = df_Total_amb.groupby(['Fecha','Tostador']).agg({"Café verde": 'sum', "Consumo gas postquemador": 'sum',"Consumo gas tostador":'sum'}).reset_index()  #.reset_index() para crear los indices y que fecha y tostador dejen de serlo gracias al groupby
# df_Tostadores_Totalizados_amb.head()

#**Archivos de Consolidados de Gas-Tomados desde MES**

# df_Tostion_MES_totales.dtypes

fecha_min = df_Tostion_MES_totales['Fecha'].min()
df_Tostion_MES_totales = df_Tostion_MES_totales[df_Tostion_MES_totales['Fecha'] != fecha_min]  #generalmente se hace cuando la primera fila tiene datos incompletos, valores nulos o de prueba, etc, se puede agregar algo para ver cuantas se eliminaron

#**Generación del archivo de Fechas en formato CSV**

df_Fechas = fc.generar_fechas("2024-01-01")

#mostrar las primeras y últimas filas
# df_Fechas.head()

###**Consolidados**

#Estos datos corresponden a los datos importados desde MES , agregando columnas de Año ,Mes, , etc , pero sin realizar ningun tipo de filtros.
#Contiene los datos crudos provenientes del sistema MES, se aplico una limpieza y tranformacion basica para estandarizar y preparar la info modificando los valores atipicos con el promedio de los valores validos
df_sn_Filtros.to_csv(RUTA_BASE + 'Detalle_TostionMDE_TOTAL_csv.csv', index=False, decimal=',')

#Contiene lo mismo que df_sn_Filtros pero con los indicadores de consumo energetico e hidricos para analisis, eliminando valores atipicos (IQR) y dejando datos confiables
df.to_csv(RUTA_BASE + 'Detalle_TostionMDE_TOTAL_Tostadores_In_csv.csv', index=False, decimal=',')

#Contiene los datos crudos lo mismo de df_sn_Filtros agrupando por Fecha y Tostador
df_Tostadores_Totalizados.to_csv(RUTA_BASE + 'Datos_Totalizados_MES_csv.csv', index=False, decimal=',')

#Contiene datos crudos directamente exportados del sistema MES tomando todos los datos menos la fecha minima
df_Tostion_MES_totales.to_csv(RUTA_BASE + 'Consumos_MES_csv.csv', index=False, decimal=',')

#Consolidado del consumo energetico de Tostador y postquemadores haciendo limpieza basica e integrandolos en un solo dataframe agregando algunas columnas para generar los indicadores de consumo energetico
df_Total_amb.to_csv(RUTA_BASE + 'Consumos_Gas_Tostion_csv.csv', index=False, decimal=',')

#Consolidado de consumo energetico de Secador haciendo limpieza basica e integrandolos en un solo dataframe agregando algunas columnas para generar los indicadores de consumo energetico
df_Total_amb_Secado.to_csv(RUTA_BASE + 'Consumos_Gas_Secado_csv.csv', index=False, decimal=',')

#Consolidado de consumo energetico de Aglomerado haciendo limpieza basica e integrandolos en un solo dataframe agregando algunas columnas para generar los indicadores de consumo energetico
df_Total_amb_Aglomerado.to_csv(RUTA_BASE + 'Consumos_Gas_Aglomerado_csv.csv', index=False, decimal=',')

#Consolidado df Total_amb pero agrupado por Fecha y Tostador sumando el consumo de gas postquemador, tostados y cantidad de cafe verde(resumen diario de lo que hizo cada tostador con respecto a energia y cafe procesado)
df_Tostadores_Totalizados_amb.to_csv(RUTA_BASE + 'Tostadores_Totalizados_Amb.csv', index=False, decimal=',')

#Se utiliza para identificar las combinaciones unicas existentes entre cada tostador y el materil(producto) que ha procesado
combinaciones_unicas.to_csv(RUTA_BASE + 'Tostador_Material.csv', index=False, decimal=',')

#Archivo de fechas desde 01-01-2024 hasta la fecha actual
df_Fechas.to_csv(RUTA_BASE + 'Fechas_csv.csv', index=False)

#Consolidado que contabiliza los bache o interrupciones que tuvo cada tostador (cada vez que el proceso no registro consumo o tuvo perdida continua de datos)
df_conteo_baches_perdidos.to_csv(RUTA_BASE + 'Conteo_Baches_Perdidos.csv', index=False)