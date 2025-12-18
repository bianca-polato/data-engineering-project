import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT_DIR))

from src.ingestion.bronze_loader import download_to_bronze

def test_download_to_bronze_creates_file(tmp_path):

    url = "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/tips.csv"
    
    bronze_dir  = tmp_path / "bronze"
    output_path = download_to_bronze(url, bronze_dir)

    assert output_path.exists()
    assert output_path.is_file()
    assert output_path.stat().st_size > 0
    assert output_path.parent == bronze_dir

