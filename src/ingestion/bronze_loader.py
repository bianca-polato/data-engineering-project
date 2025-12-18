from pathlib  import Path
from datetime import datetime

import requests

def download_to_bronze(url: str, bronze_dir: Path) -> Path:
    """
    Download de CSV a partir da URL e salva na camada bronze
    
    - url: URL para um arquivo CSV.
    - bronze_dir: diretório base da camada bronze (ex.: Path("data/bronze")).

    retorna: Path completo do arquivo salvo.
    """

    bronze_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"raw{timestamp}.csv"
    output_path = bronze_dir / file_name

    output_path.write_bytes(response.content)
    
    return output_path