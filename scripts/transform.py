import pandas as pd

def transform():
    # Carga los datos extraídos previamente
    df = pd.read_csv("/opt/airflow/scripts/data.csv")

    # Limpia nombres de columnas (como ejemplo)
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    # Guarda el archivo limpio
    df.to_csv("/opt/airflow/scripts/data_clean.csv", index=False)
    print("✔ Datos transformados.")
