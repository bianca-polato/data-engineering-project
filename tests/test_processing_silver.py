import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from src.processing.silver_tips import transform_tips, save_silver, process_bronze_to_silver


def test_transform_tips_cleans_and_normalizes():
    raw = pd.DataFrame(
        {
            "Total Bill": [10.0, 20.0, 20.0],
            "Tip": [1.0, None, 2.0],
            "size": ["2", "3", "3"],
        }
    )

    df = transform_tips(raw)

    # nomes de colunas
    assert set(df.columns) == {"total_bill", "tip", "size"}

    # sem nulos em colunas críticas
    assert df["tip"].isna().sum() == 0

    # tipos numéricos
    assert pd.api.types.is_numeric_dtype(df["total_bill"])
    assert pd.api.types.is_numeric_dtype(df["size"])

    # duplicatas removidas (tinha 3 linhas, mas 2 iguais)
    assert len(df) == 2


def test_save_silver_creates_csv(tmp_path):
    df = pd.DataFrame({"a": [1, 2]})
    silver_dir = tmp_path / "silver"

    output_path = save_silver(df, silver_dir)

    assert output_path.exists()
    assert output_path.suffix == ".csv"
    assert output_path.parent == silver_dir

def test_process_bronze_to_silver(tmp_path):

    bronze_path = tmp_path / "bronze" / "fake_bronze.csv"
    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
         "Total Bill": [16.99, 10.34]
        ,"tip": [1.01, None]
        ,"size": [2, 2]
    }).to_csv(bronze_path, index=False)

    silver_dir = tmp_path / "silver"

    #executa fluxo completo 
    silver_path = process_bronze_to_silver(bronze_path, silver_dir)

    #verifica resultados 
    assert silver_path.exists()
    df_result = pd.read_csv(silver_path)
    assert len(df_result) == 1
    assert "total_bill" in df_result.columns