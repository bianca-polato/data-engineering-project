from datetime import datetime
from pathlib import Path

import pandas as pd

def load_bronze_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)

def transform_tips(df: pd.DataFrame) -> pd.DataFrame:
    #padronizar nome de colunas
    df = df.copy()
    df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

    #tipos numericos 
    numeric_cols = ["total_bill", "tip", "size"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    
    #remover linhas com valores nulos em colunas chaves
    df = df.dropna(subset=["total_bill", "tip"])

    #remover duplicatas
    df = df.drop_duplicates().reset_index(drop=True)

    return df

def save_silver(df: pd.DataFrame, silver_dir: Path) -> Path:
    silver_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"tips_silver_{timestamp}.csv"
    output_path = silver_dir / file_name
    df.to_csv(output_path, index=False)
    return output_path

def process_bronze_to_silver(bronze_file: Path, silver_dir: Path) -> Path:
    #carrega bronze
    df = load_bronze_csv(bronze_file)

    #transforma 
    df_clean = transform_tips(df)

    #salva silver
    return save_silver(df_clean, silver_dir)
