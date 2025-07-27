import io
import json
import re
import zipfile
from datetime import date
from pathlib import Path
from typing import Dict

import polars as pl
import requests


def download_and_extract_zip(url: str, target_dir: Path) -> Path:
    """
    Download and extract a zip file from a URL.

    Args:
        url: URL of the zip file
        target_dir: Directory to extract the contents to
    """
    target_dir.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True)
    try:
        response.raise_for_status()
    except requests.HTTPError as e:
        raise requests.HTTPError(f"Falha ao baixar o arquivo: {e}")

    try:
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            zf.extractall(path=target_dir)
    except zipfile.BadZipFile as e:
        raise zipfile.BadZipFile(f"O arquivo baixado não é um ZIP válido: {e}")
    return target_dir


def load_datapackage(datapackage_path: Path) -> Dict:
    """
    Load and parse a datapackage.json file.

    Args:
        datapackage_path: Path to the datapackage.json file

    Returns:
        Dictionary containing the datapackage metadata
    """
    with open(datapackage_path, "r", encoding="utf-8") as f:
        return json.load(f)


def today_label() -> str:
    """Return today's date in the format DDMMYYYY."""
    return date.today().strftime("%d%m%Y")


def merge_csvs_to_parquet(
    data_dir: Path, output_file: str, drop_columns=None, **read_csv_kwargs
):
    """Given a directory with csv files, merge them into a single parquet file."""
    data_dir_glob = f"{data_dir}/*.csv"
    df = pl.read_csv(data_dir_glob, **read_csv_kwargs)
    if drop_columns:
        df = df.drop(drop_columns)
    df.write_parquet(output_file)
    return output_file


def normalize_column_name(text: str) -> str:
    from unidecode import unidecode

    normalized_description = unidecode(text)
    normalized_description = re.sub(r"[^\w]", " ", normalized_description)
    normalized_description = normalized_description.lower().replace(" ", "_")
    normalized_description = re.sub(r"_+", "_", normalized_description)
    if normalized_description.endswith("_"):
        normalized_description = normalized_description[:-1]
    return normalized_description
