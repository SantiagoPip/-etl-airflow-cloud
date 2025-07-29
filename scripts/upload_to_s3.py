import pandas as pd

def load():
    df = pd.read_csv("/opt/airflow/scripts/data_clean.csv")
    print("✔ Datos listos para cargar (simulado):")
    print(df.head())
