"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

# python -m venv .venv
# .venv\Scripts\activate
# python.exe -m pip install --upgrade pip
# pip3 install pyarrow pandas

import pandas as pd


def ingest_data():

    # Leemos el archivo como ancho fijo
    df = pd.read_fwf("clusters_report.txt",widths=[9, 16, 16, 77])
    
    # Corregimos los nombres de las columnas
    df.columns = df.columns + " " + list(df.iloc[0])
    df.columns = [columna.replace(" nan", "").replace(" ", "_").lower() for columna in df.columns]
    
    # Eliminamos las dos primeras filas
    df = df.iloc[2:]
    df.reset_index(inplace=True, drop=True)
    
    # Corregimos la columna de principales_palabras_clave
    df.iloc[23,3] = df.iloc[23,3] + '.'
    columna = list(df.iloc[:,3]).copy()
    nueva_columna = []
    temp_str = ""

    for item in columna:
        if temp_str:         # Añadir el elemento actual a la cadena temporal
            temp_str += ' ' + item
        else:
            temp_str = item
        
        # Si el elemento actual termina en un punto, añadir a la nueva lista y resetear la cadena temporal
        if item.endswith('.'):
            nueva_columna.append(temp_str)
            temp_str = ""  # Resetear para el próximo grupo

    # Filtramos solo las filas donde 'cluster' no es NaN
    df = df[df['cluster'].notna()]
    df['principales_palabras_clave'] = nueva_columna
    
    # Eliminamos los dobles espacios
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('    ', ' ').str.replace('   ', ' ').str.replace('  ', ' ').str.replace(',,', ',').str.replace('.', '')
    df.reset_index(inplace=True, drop=True)
    
    # Convertimos las columnas cluster y cantidad_de_palabras_clave a numéricas
    df['cluster'] = df['cluster'].astype(int)
    df['cantidad_de_palabras_clave'] = df['cantidad_de_palabras_clave'].astype(int)
    
    # De la columna porcentaje_de_palabras_clave removemos de todos los registros el " %" y convertimos a float
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(' %', '').str.replace(",",".").astype(float)
    
    return df
