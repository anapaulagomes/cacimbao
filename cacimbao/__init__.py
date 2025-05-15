import os
import zipfile
import json
import tempfile
from pathlib import Path
from typing import Literal, Dict, List, Tuple
import narwhals as nw
import requests


DATASETS_DIR = Path(__file__).parent / "datasets"  # TODO check best practices to store data locally

DATASETS_METADATA: Dict[str, Dict] = {
    "filmografia_brasileira": {
        "name": "filmografia_brasileira",
        "size": "medium",  # small / medium / large  # TODO establish a standard for this
        "description": "Brazilian filmography dataset from Cinemateca Brasileira",
        "local": False,
        "download_url": "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-brasileira.zip",
        "filename": "filmografia_brasileira.csv"  # FIXME filename in the zip file
    },
}

def _download_and_extract_zip(url: str, target_dir: Path) -> None:
    """
    Download and extract a zip file from a URL.
    
    Args:
        url: URL of the zip file
        target_dir: Directory to extract the contents to
    """
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp_file:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        for chunk in response.iter_content(chunk_size=8192):
            tmp_file.write(chunk)
        
        tmp_file.flush()
        
        with zipfile.ZipFile(tmp_file.name, 'r') as zip_ref:
            zip_ref.extractall(target_dir)
        
        os.unlink(tmp_file.name)


def _load_datapackage(datapackage_path: Path) -> Dict:
    """
    Load and parse a datapackage.json file.
    
    Args:
        datapackage_path: Path to the datapackage.json file
        
    Returns:
        Dictionary containing the datapackage metadata
    """
    with open(datapackage_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def _get_dataset_metadata(name: str) -> Dict:
    """
    Get metadata for a dataset, including datapackage information if available.
    
    Args:
        name: Name of the dataset
        
    Returns:
        Dictionary containing the dataset metadata
    """
    metadata = DATASETS_METADATA[name].copy()
    
    # if the dataset is not local and we have a datapackage.json, load its metadata
    if not metadata["local"]:
        datapackage_path = DATASETS_DIR / "datapackage.json"
        if datapackage_path.exists():
            datapackage = _load_datapackage(datapackage_path)
            if datapackage.get("resources"):
                resource = datapackage["resources"][0]
                metadata.update({
                    "description": datapackage.get("description", metadata["description"]),
                    "size": f"{resource.get('bytes', 0) / 1024 / 1024:.1f}MB",
                    "filename": resource["path"]
                })
    
    return metadata


def list_datasets() -> List[str]:
    # TODO add option to return metadata
    return list(DATASETS_METADATA.keys())


def download_dataset(
    name: str,
    df_format: Literal["polars", "pandas"] = "polars"
):
    """
    Download and load a dataset.
    
    Args:
        name: Name of the dataset to download
        df_format: Format of the returned dataframe ("polars" or "pandas")
        
    Returns:
        DataFrame in the specified format
    """
    if name not in DATASETS_METADATA:
        raise ValueError(f"Base de dados '{name}' não encontrada. Use list_datasets() para ver as bases disponíveis.")
    
    dataset_info = DATASETS_METADATA[name]
    filename = dataset_info["filename"]
    
    if dataset_info["local"]:
        file_path = DATASETS_DIR / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Local dataset '{name}' not found at {file_path}")
    else:
        # Handle remote datasets  # TODO extract to a function
        file_path = DATASETS_DIR / name
        if not file_path.exists():
            file_path.mkdir(parents=True, exist_ok=True)

            _download_and_extract_zip(dataset_info["download_url"], DATASETS_DIR)

            # load the datapackage.json to get the correct filename
            datapackage_path = file_path / "datapackage.json"
            if datapackage_path.exists():
                datapackage = _load_datapackage(datapackage_path)

                if datapackage.get("resources"):
                    filename = datapackage["resources"][0]["path"]
                    file_path = DATASETS_DIR / filename
    
    df = nw.read_csv(file_path)

    if df_format == "pandas":
        return df.to_pandas()
    return df.to_polars()
