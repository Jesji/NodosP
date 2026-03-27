import pandas as pd
import glob as glob 
import numpy as np 
from functions import *

imprimir_bienvenida()
print("""\nEn este script se limpian los datos de los catálogos de NodosP en
      la carpeta "DownloadCatalogos/", se estandarizan las claves de 
      entidad y municipio. Se leen los datos de latitud y longitud de 
      cada municipio del archivo "AGEEML_202512111134444.xlsx" que 
      corresponde al Catálogo Único de Claves de Áreas Geoestadísticas 
      Estatales, Municipales y Localidades publicado por el INEGI 
      (https://www.inegi.org.mx/app/ageeml/). Finalmente se concatenan 
      todos los catálogos de NodosP descargados con sus datos de fecha 
      de publicación y ubicación, y se exportan en el archivo 
      "df_nodosP.parquet". """)
# leer datos de municipio inegi
poblaciones_file = r'Data/AGEEML_202512111134444.xlsx'
df_poblaciones = importar_munis(poblaciones_file)
# agrupar por municipio y exportar diccionarios para limpiar datos
df_mun_agg, dict_nommun_cvemun_canon , dict_cvemun_nommun_canon = agrupar_mun(df_poblaciones)
export_file = 'Data/df_munis.parquet'
df_mun_agg.to_parquet(export_file, engine='pyarrow',compression='snappy')
print(f"Datos de municipios leidos y exportados a {export_file}")

# importar catalogos, formatear y concatenar archivos
folder_path = "DownloadCatalogos/"
df_catalogo = read_concat(folder_path)
df_catalogo = formatear_texto_columnas(df_catalogo)
df_catalogo = reduce_columns(df_catalogo)
# agrupar por clave y quitar los clave donde hay todos nan y no aplica
df_cat_agg  = agrupar_por_clave(df_catalogo)
df_cat_agg, df_cat_noaplica = drop_noaplica(df_cat_agg)
df_cat_agg, df_cat_allna = drop_all_nan(df_cat_agg)
# en caso de que falten cve_ent y cve_mun, buscar los valores 
if ('NOM_EST' and 'NOM_LOC') in df_cat_agg.columns:
    # arreglar valores donde falta cveent pero hay nomest
    df_cat_agg_fixed = fix_missing_cve_ent(df_cat_agg)
    df_cat_agg_fixed = fix_missing_cvemun(df_cat_agg_fixed,
                    dict_cvemun_nommun_canon=dict_cvemun_nommun_canon,
                    dict_nommun_cvemun_canon=dict_nommun_cvemun_canon)
else: df_cat_agg_fixed = df_cat_agg.copy()
df_cat_agg_fixed = quitar_cvemun_duplicados(df_cat_agg_fixed)
# solo dejar las columnas necesarias
df_cat_agg_fixed = keep_cols_clean(df_cat_agg_fixed)
file_cat_ag = 'Data/df_cat_agg.parquet'
df_cat_agg_fixed.to_parquet(file_cat_ag, engine='pyarrow', compression='snappy')
print(f"\nEl df de catalogo agrupado por clave fue exportado al archivo {file_cat_ag}")


# ahora con el catalogo de nodos agrupado se estarizan los valores de los catalogos completos 
# solo se mantienen columnas de interes
cols_to_keep = ['SISTEMA',
                'CCR',
                'ZONA DE CARGA', 'CLAVE',
                'TENSION',
                'CVE_ENT', 'NOM_ENT',
                'CVE_MUN','NOM_MUN', 'Date'
                ]
df_catalogo_fixed = df_catalogo[cols_to_keep]
# se mapean valores faltantes usando el catalogo agrupado
df_catalogo_fixed = mapear_faltantes_catalog(df_catalogo_fixed,                                           df_cat_agg_fixed)
# se retiran valores con no aplica
df_catalogo_fixed, df_no_aplica = drop_noaplica(df_catalogo_fixed)
# se hace un merge del df de catalogos completo con el de municipios, 
# esto produce un df con datos de ubicacion 
df_for_analysis = df_catalogo_fixed.merge(df_mun_agg,how='left',
                                on=['CVE_ENT','CVE_MUN','NOM_ENT'])
# se cambian los nombres de algunas columnas
df_for_analysis_fixed = (df_for_analysis.copy()
                        .drop(columns=['NOM_MUN_x'])    
                        .rename(columns={'NOM_MUN_y':'NOM_MUN', 
                                        'ZONA DE CARGA':'ZC'}))
# exportar df para analisis a parquet
file_nodos = 'Data/df_nodosp.parquet'
df_for_analysis_fixed.to_parquet(file_nodos, engine='pyarrow',compression='snappy')
print(f"""\nEl df del catalogo de nodos estandarizado ha sido exportado 
      al archivo {file_nodos}""")

