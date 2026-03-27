import pandas as pd 
import geopandas as gpd
import matplotlib.pyplot as plt 
import json
from matplotlib.colors import ListedColormap
from functions import * 
from Graphics.dictcolores import dict_ccr_color

# import data from nodosp y mapa
# df = pd.read_parquet('Data/df_nodosp.parquet')
mapa = gpd.read_file(r'Graphics/Mexico_mun/00mun' + '.shp')
# df_catalogo = pd.read_parquet('Data/df_catalogo.parquet')
df_municipios = pd.read_parquet('Data/df_munis.parquet')
mapa['CVE_ENT'] = mapa['CVE_ENT'].astype(int)
mapa['CVE_MUN'] = mapa['CVE_MUN'].astype(int)
# df_municipios.info()
# mapa.info()
# se hace el merge 
df_mun_ccr = mapa.merge(df_municipios, how='left', on=['CVE_ENT','CVE_MUN'])
# el archivo dictCCR-delimitacion.py contiene las delimitaciones de municipios para cada CCR
dict_file = 'Graphics/dictCCRdelimitacion.py'
df_mun_ccr['CCR'] = df_mun_ccr.apply(delimitar_CCR, axis=1, dict_file=dict_file)  
# crear un mapa de colores con el dict importado del archivo dictCCRcolor.py anterior 
cmap = ListedColormap([dict_ccr_color[k] for k in dict_ccr_color])
# asegúrate de que el orden de categorías coincide con el de cmap
df_mun_ccr['CCR'] = pd.Categorical(df_mun_ccr['CCR'], categories=list(dict_ccr_color))
# Graficar con mejor estilo 
fig, ax = plt.subplots(1,figsize=(10,6), dpi=500)
df_mun_ccr.plot(ax=ax,column='CCR',cmap=cmap, legend=True,
                legend_kwds={'fontsize':12,},)
plt.axis('off')
plt.figtext(0.2,0.95,
        "Centros de Control Regional CENACE",
        fontweight = 'bold', fontsize=20,
        color = "#525252")
plt.figtext(0.3, 0.05, "Fuente: Elaboración propia con datos de INEGI y del DOF",
            color = "#525252",
            fontsize = 12) # Pie de gráfico
path = 'Graphics/mapCCR.png'
plt.savefig(path,dpi = 500,bbox_inches = 'tight',facecolor='white' )
print(f"Mapa de los Centros de Control Regional del CENACE exportado correctamente a {path}")