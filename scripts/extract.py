import requests
import pandas as pd
from io import StringIO

def extract():
    url = "https://people.sc.fsu.edu/~jburkardt/data/csv/hw_200.csv"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception("Error al descargar el archivo CSV")
    
    df = pd.read_csv(StringIO(response.text))
    df.to_csv("/opt/airflow/scripts/data.csv", index=False)
    print("✔ Datos extraídos y guardados.")
