def consulta_tostadores_2025(fecha_inicio, fecha_fin_str):
    return f'''
    SELECT *
    FROM eMesOS3.dbo.VW_NCF_CLI_14_BI_BACHES_TOSTION_TEMP
    WHERE FechaHora >= '{fecha_inicio}' AND FechaHora <= '{fecha_fin_str}'
    '''


def consulta_consumo_MES(fecha_inicio, fecha_fin_str):
    return f'''
    SELECT *
    FROM eMesOS3.dbo.VW_NCF_CLI_14_COB_REP_DATA_PPA_PARAMS_SSEE_CM10
    WHERE Fecha >= '{fecha_inicio}' AND Fecha <= '{fecha_fin_str}'
    '''
