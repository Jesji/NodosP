import pandas as pd 
import geopandas as gpd
import matplotlib.pyplot as plt 
import json
import plotly.express as px
from Graphics.dictcolores import dict_tension_color, dict_ccr_color, graf_esp
from functions import imprimir_bienvenida

imprimir_bienvenida()
print("""\nEn este script se toma el archivo Data/df_nodosp.parquet que 
      contiene los catalogos de NodosP concatenados y se realizan los 
      siguientes análisis:
      - Se agrupan por CCR y por año para observar como ha variado el 
      número de nodosP. Se exporta la figura "Graphics/NodosP-CCR.png" y 
      la tabla "Data/NodosP-CCR.xlsx".
      - Se agrupan los catalogos por año y por nivel de tensión y por CCR,
      se producen las figuras 'Graphics/nodosp-tension.png' y 'Graphics
      /NodosP-ccr-tension.png'.
      - Se produce un mapa interactivo de la ubicación de los NodosP por 
      nivel de tensión cada año, se guarda en el archivo 'Graphics/MapaInter.html'.
      Y tambien se guardan imagenes por cada nivel de Tensión con el formato
      de nombre 'Graphics/ubi-{tension}{year}.png'
      """)
# import data from nodosp y mapa
df = pd.read_parquet('Data/df_nodosp.parquet')
# extraer columna year
df_plot = df.copy()
df_plot['YEAR'] = df_plot['Date'].dt.year
df_plot['MONTH'] = df_plot['Date'].dt.month
df_plot['TENSION'] = df_plot['TENSION'].astype(str)

#agrupar por año, ccr
df_ag_ccr = df_plot.groupby(['YEAR','CCR']).agg(
    NodosP = ('CLAVE', 'nunique')
).reset_index()

df_ag_ccr['Total_anual'] = df_ag_ccr.groupby(['YEAR'])['NodosP'].transform('sum')
df_ag_ccr['Porc'] = (100* df_ag_ccr['NodosP'] / df_ag_ccr['Total_anual']).map("{:.1f}%".format)

# grafica de barras nodosP por ccr
fig = px.bar(
    df_ag_ccr,
    x="YEAR",
    y="NodosP",
    color="CCR",
    color_discrete_map=dict_ccr_color,
    barmode="stack",  # cambia a "group" si prefieres barras lado a lado
    hover_data = {'Porc':True},#text = 'Porc',#title="NodosP por CCR", width=600, height=400
    )

fig.update_layout(
    template="plotly_white",
    font=dict(family="Arial", size=12, color="#2b2b2b"),
    title=dict(text= "<b>NodosP por CCR anual</b>", x=0.05, y = 0.97, xanchor="left", font=dict(size=20, color = "#525252")),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(l=0, r=0, t=90, b=90),
    legend=dict(title = None,
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="left",
        x=0.0,
        font=dict(size=12),
    ),
    annotations = [
        dict(
            text="Fuente: Elaboración propia con datos de CENACE ",
            x=0,
            y=-0.23,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=11, color="#6b6b6b"),
            align="left"
        )
    ]
    )
fig.update_xaxes(
        title=dict(text = "AÑO", font=dict(size=16)),  tickangle=45, dtick = 1, 
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False, 
    )

fig.update_yaxes(
        title=dict(text = "NodosP", font=dict(size=16)),
        showgrid=True,
        zeroline=False
    )

# Bordes sutiles en barras para look institucional
fig.update_traces(marker_line_width=0.5, marker_line_color="rgba(0,0,0,0.25)")

path_save = 'Graphics/NodosP-CCR.png'
path_saveht = 'Graphics/NodosP-CCR.html'
fig.write_image(path_save)
fig.write_html(path_saveht)

# pivo table sumando por CCR
piv_agg_ccr = pd.pivot_table(df_ag_ccr, values='NodosP', aggfunc='sum', 
            columns='YEAR', index='CCR', margins=True, margins_name='Total')
min_year = min(df_plot['YEAR'])
max_year = max(df_plot['YEAR'])
piv_agg_ccr['Tasa'] = (piv_agg_ccr.loc[:,max_year] - piv_agg_ccr.loc[:,min_year])/piv_agg_ccr.loc[:,min_year]
piv_agg_ccr['Cambio'] = (piv_agg_ccr.loc[:,max_year] - piv_agg_ccr.loc[:,min_year])
piv_agg_ccr['comp_ini'] = piv_agg_ccr.loc[:,min_year] / piv_agg_ccr.loc['Total',min_year]
piv_agg_ccr['comp_fin'] = piv_agg_ccr.loc[:,max_year] / piv_agg_ccr.loc['Total',max_year]
# print(piv_agg_ccr)
piv_agg_ccr.to_excel("Data/NodosP-CCR.xlsx")

# orden de tension
order_tension = sorted(df_plot['TENSION'].dropna().unique(),key=lambda x: float(x))

# agrupar por nivel de tension
df_ag_tens = (df_plot.copy().groupby(['YEAR','TENSION']).agg(
    NodosP = ('CLAVE','nunique'))
    .reset_index() )

# Calcular porcentajes del total
df_ag_tens['Total_year'] = df_ag_tens.groupby(['YEAR'])['NodosP'].transform('sum')
df_ag_tens['Porcen'] = (100 * df_ag_tens['NodosP'] / df_ag_tens['Total_year']).map("{:.1f}%".format)

# graficar
fig = px.bar(
    df_ag_tens,
    x = 'YEAR',
    y = 'NodosP',
    color = 'TENSION',
    barmode='stack',
    color_discrete_map=dict_tension_color,
    category_orders={'TENSION':order_tension,},
    hover_data={'Porcen':True}
)
fig.update_layout(
    template="plotly_white",
    font=dict(family="Arial", size=12, color="#2b2b2b"),
    title=dict(text= "<b>NodosP por TENSION (kV) anual</b>", x=0.05, y = 0.97, xanchor="left", font=dict(size=20, color = "#525252")),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(l=0, r=0, t=90, b=90),
    legend=dict(title = None,
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="left",
        x=-0.05,
        font=dict(size=12),
    ),
    annotations = [
        dict(
            text="Fuente: Elaboración propia con datos de CENACE ",
            x=0,
            y=-0.23,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=11, color="#6b6b6b"),
            align="left"
        )
    ]
    )
fig.update_xaxes(
        title=dict(text = "AÑO", font=dict(size=16)),  tickangle=45, dtick = 1, 
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False, 
    )
fig.update_yaxes(
        title=dict(text = "NodosP", font=dict(size=16)),
        showgrid=True,
        zeroline=False
    )
fig.update_traces(marker_line_width=0.5, marker_line_color="rgba(0,0,0,0.25)")
path = 'Graphics/nodosp-tension.png'
pathtm = 'Graphics/nodosp-tension.html'
fig.write_html(pathtm)
fig.write_image(path)

# agrupar por CCR, año y tension
df_ag_ccr_tens = df_plot.groupby(['YEAR','CCR','TENSION']).agg(
    NodosP = ('CLAVE','nunique')
).reset_index()
df_ag_ccr_tens['Total_year_tens'] = df_ag_ccr_tens.groupby(['YEAR','TENSION'])['NodosP'].transform('sum')
df_ag_ccr_tens['%_YT'] = (100 * df_ag_ccr_tens['NodosP'] / df_ag_ccr_tens['Total_year_tens']).map("{:.1f}%".format)
fig = px.bar(
    df_ag_ccr_tens,
    x="YEAR",
    y="NodosP",
    color="TENSION",
    facet_col="CCR",
    color_discrete_map=dict_tension_color,
    barmode="relative",  # cambia a "group" si prefieres barras lado a lado
    category_orders={"TENSION":order_tension},
    hover_data={'%_YT':True}
    #title="NodosP por CCR", width=600, height=400
    )

fig.update_layout(
    template="plotly_white",
    font=dict(family="Arial", size=12, color="#2b2b2b"),
    title=dict(text= f"<b>NodosP por CCR y TENSION (kV) anual {min_year}-{max_year}</b>", x=0.05, y = 0.97, xanchor="left", font=dict(size=20, color = "#525252")),
    plot_bgcolor="white",
    paper_bgcolor="white",
    margin=dict(l=0, r=20, t=90, b=120),
    legend=dict(title = None,
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="left",
        x=-0.05,
        font=dict(size=12),
    ),
    annotations = [
        dict(
            text="Fuente: Elaboración propia con datos de CENACE ",
            x=0.2,
            y=-0.4,
            xref="paper",
            yref="paper",
            showarrow=False,
            font=dict(size=11, color="#6b6b6b"),
            align="left"
        )
    ]
    )

# eje x
fig.update_xaxes(
        #title=dict(text = "", font=dict(size=16)),  tickangle=45, dtick = 1, 
        title = dict(text= ""),
        showticklabels = False,
        showgrid=True,
        gridcolor="rgba(0,0,0,0.08)",
        zeroline=False, 
    )
# labels CCR 
list_x = [0.02,0.14,0.25,0.37,0.48,0.60,0.71,0.82,0.93]
for i,label in enumerate(df_ag_ccr_tens['CCR'].unique()):
    fig.add_annotation(
        text = f"{label}",
        y=-0.0,
        x=list_x[i],
        xref = "paper", 
        yref = "paper",
        showarrow=False,
        textangle=45,
        xanchor='left',
        yanchor='top',
        align='left',
        font=dict(size=10)
        
    )
# Bordes sutiles en barras para look institucional
fig.update_traces(marker_line_width=0.5, marker_line_color="rgba(0,0,0,0.25)")
path = 'Graphics/NodosP-ccr-tension.png'
pathtml = 'Graphics/NodosP-ccr-tension.html'
fig.write_image(path, scale=3)
fig.write_html(pathtml,)

# Producir mapa interactivo con ubicacion de nodos
df_dash = df_plot.copy()
df_dash['YEAR'] = df_dash['Date'].dt.to_period('Y').astype(str) 
df_y = df_dash.sort_values('Date').groupby(['YEAR','CLAVE'],as_index=False ).tail(1)
df_y['numNodospTens'] = df_y.groupby(['YEAR','CCR','TENSION','CVE_ENT','CVE_MUN',])['CLAVE'].transform('nunique')
# replace nan values de lat con 0 en el conteo
df_y['numNodospTens'] = df_y['numNodospTens'].fillna(0)
# graficar agregando animation frame
fig = px.scatter_map(
    df_y,
    lat = 'LAT_DECIMAL',
    lon = 'LON_DECIMAL',
    color = 'TENSION',
    size='numNodospTens',
    size_max=20,
    color_discrete_map=dict_tension_color,
    hover_name='NOM_MUN',
    hover_data={
        'CCR':True,
        "LAT_DECIMAL":False,
        "LON_DECIMAL":False,
        "NOM_ENT":True,
        "NOM_MUN":True,
        "YEAR":False,
        "TENSION":False,
        "numNodospTens":True
    },
    labels={"numNodospTens":"NodosP"},
    category_orders={'TENSION':order_tension},
    zoom=3.5,
    width=610,
    height=480,
    center = {"lat":23.5, "lon":-102},
    animation_frame='YEAR'
)

fig.update_layout(
    map_style = 'carto-positron',
    margin = dict(l=5,r=60,t=40,b=0),
    #template="plotly_white",
    font=dict(family="Arial", size=12, color="#2b2b2b"),
    title=dict(text= "<b>Ubicación NodosP por TENSION (kV) anual</b>", x=0.05, y = 0.97, xanchor="left", font=dict(size=20, color = "#525252")),
    plot_bgcolor="white",
    paper_bgcolor="white",
    legend=dict(title = None,
        orientation="v",
        yanchor="top",
        y=1,
        xanchor="left",
        x=1,
        font=dict(size=15),
    )
)
# ajustar slider
fig.update_layout(
    sliders=[dict(
        active=0,
        currentvalue={"prefix": "Año: "},
        y = 0.17
    )],
    updatemenus = [dict(
        y = 0.17, 
        showactive = True
    )]
)
path = 'Graphics/MapaInter.html'
fig.write_html(path,)

# graficar casos por tension 
graf_esp(df_y,tension='138.0',year=str(max_year))
graf_esp(df_y,tension='115.0',year=str(max_year))
graf_esp(df_y,tension='85.0',year=str(max_year))
graf_esp(df_y,tension='69.0',year=str(max_year))
graf_esp(df_y,tension='400.0',year=str(max_year))
graf_esp(df_y,tension='230.0',year=str(max_year))

print("Gráficos producidos. Rutina terminada con exito")