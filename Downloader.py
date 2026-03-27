from pathlib import Path
import urllib.parse
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time
from functions import * 

# imprimir intro 
imprimir_bienvenida()
print("""
      \nEste script descarga todos los catalogos publicados en el 
      sitio https://www.cenace.gob.mx/Paginas/SIM/NodosP.aspx en el 
      intervalo de años dado. Se creará la carpeta "DownloadCatalogos/"
      en la que se descargarán todos los archivos en formato .xlsx. El
      rango de años con datos es de 2016 en adelante. 
      """)
in_year = int(input("Ingrese año inicial: " ))
fn_year = int(input('Ingrese año final: ' ))
# crear lista de años
years_list = list()
[years_list.append(in_year+year) for year in range(fn_year - in_year + 1)]

# dar url
url = "https://www.cenace.gob.mx/Paginas/SIM/NodosP.aspx"

# crear donwload folder
folder = "DownloadCatalogos"
download_folder = create_folder(folder) # crear folder para descargar

# set params for selenium
options = Options()
prefs = {'download.default_directory':str(download_folder)}
options.add_experimental_option("prefs",prefs)
options.add_argument("--headless=new") # para que no se abra ventana
driver = webdriver.Chrome(options=options)

# por cada anio descargar archivos
for year in years_list:
    try:
        print(f"Iniciando descarga de {year}")
        driver.get(url=url)
        # encontrar path del boton para elegir anio
        year_xpath = '//*[@id="ContentPlaceHolder1_DrpAnio"]'
        textbox = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.XPATH, year_xpath))) 
        textbox.send_keys(str(year)) 
        time.sleep(2)
        num_enlaces = len(driver.find_elements(By.XPATH, "//a[contains(@href, '.xlsx')]"))
        print(f"Enlaces encontrados: {num_enlaces}")
        # descargar cada archivo en la lista de enlaces
        for i in range(num_enlaces):
            file = driver.find_elements(By.XPATH, "//a[contains(@href, '.xlsx')]")[i]
            file.click()
            print(f"Descargando archivo {i+1}/{num_enlaces}")
            time.sleep(3)
    except Exception as e: 
        print(f"fallo descarga de {year}: {e}")
        continue # si falla continua
    finally:
        print(f"Todos los archivos de {year} descargados")
# cerrar navegador
driver.quit()