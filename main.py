import boto3
from datetime import datetime
import pandas as pd
from io import BytesIO
import streamlit as st
import pytz


session = boto3.client('s3',
    aws_access_key_id = st.secrets["aws"]["aws_access_key_id"],
    aws_secret_access_key = st.secrets["aws"]["aws_secret_access_key"],
     region_name='us-east-2'
)

# Especifica el nombre del bucket y la clave del archivo .pkl en S3
def get_data(fecha_buscar):
    data_general = pd.DataFrame()
    bucket_name = 's3-pernexium-report'
    key_folder = f'raw/didi/didi_agent/{fecha_buscar}/'
    
    response = session.list_objects_v2(Bucket=bucket_name, Prefix=key_folder)
        
    if 'Contents' in response:
        # Itera sobre cada archivo en el folder
        for obj in response['Contents']:
            key = obj['Key']
            print(f"Leyendo el archivo: {key}")
    
            # Obtén el objeto desde S3
            file_response = session.get_object(Bucket=bucket_name, Key=key)
            
            # Lee el contenido del archivo en bytes
            columnas_bytes = file_response['Body'].read()
    
            # Usa BytesIO para leer el contenido como un DataFrame de pandas
            df = pd.read_csv(BytesIO(columnas_bytes))
    
            # Realiza cualquier operación que necesites con el DataFrame
            data_general = pd.concat([data_general, df])
    else:
        print(f"No se encontraron archivos en el folder {key_folder} del bucket {bucket_name}.")
        return None

    data_general = data_general.sort_values(by = "last_update", ascending=False).drop_duplicates(subset = ["agent_number"], keep="first")
    data_general['progress'] = data_general.current_page.apply(lambda x: int(x.split("/")[0]) / int(x.split("/")[1]))

    data_general = data_general[['agent_number', 'last_update', 'last_status', 'current_page', 'progress', 'errors']]
    data_general = data_general.sort_values(by = 'agent_number')
    return data_general

st.set_page_config(
    page_title="Pernexium Agentes Automáticos",
    page_icon="./img/logo_pernexium.png"  # Puedes usar una ruta local o una URL
)

st.header("Interfaz de control para agentes automáticos")

mexico_city_tz = pytz.timezone('America/Mexico_City')

# Obtén la fecha y hora actual en la zona horaria de Ciudad de México
hoy = datetime.now(mexico_city_tz).date()

# Selector de fechas con la fecha de hoy como valor predeterminado
col1, col2 = st.columns([9, 1])
with col1:
    fecha_seleccionada = st.date_input("Seleccione una fecha:", hoy)
with col2:
    #st.write("#")
    st.button('🔄')

data = get_data(fecha_seleccionada)
    
if data is None:
    st.warning("No hay información para la fecha seleccionada")
else:
    st.data_editor(data, disabled = True, 
                   column_config={
                    "progress": st.column_config.ProgressColumn(
                        "Progress",
                        help="Progreso",
                        #format="%f",
                        min_value=0,
                        max_value=1,
                    ),
                },
                hide_index=True,)



