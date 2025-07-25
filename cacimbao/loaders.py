from importlib.resources import files
from pathlib import Path
from typing import Literal

import narwhals as nw

from cacimbao.datasets import get_dataset
from cacimbao.helpers import download_and_extract_zip, load_datapackage

DATASETS_DIR = Path.home() / "cacimbao"
DATASETS_DIR.mkdir(parents=True, exist_ok=True)


def download_dataset(
    name: str, df_format: Literal["polars", "pandas"] = "polars"
) -> nw.DataFrame:
    """
    Download and load a dataset.

    Args:
        name: Name of the dataset to download
        df_format: Format of the returned dataframe ("polars" or "pandas")

    Returns:
        DataFrame in the specified format
    """
    dataset_info = get_dataset(name)

    if dataset_info.local:
        file_path = files("cacimbao.data").joinpath(dataset_info.filepath)

        if not file_path.exists():
            raise FileNotFoundError(f"Local dataset '{name}' not found at {file_path}")
    else:
        file_path = DATASETS_DIR / name
        file_path = download_and_extract_zip(dataset_info.download_url, file_path)

        # load the datapackage.json to get the correct filename
        datapackage = load_datapackage(file_path / "datapackage.json")
        filename = datapackage["path"]
        file_path = file_path / filename

    if file_path.suffix == ".csv":
        df = nw.read_csv(file_path, backend=df_format)
    elif file_path.suffix == ".parquet":
        df = nw.read_parquet(file_path, backend=df_format)
    else:
        raise ValueError(f"Formato de arquivo não suportado: {file_path.suffix}")

    if df_format == "pandas":
        return df.to_pandas()
    return df.to_polars()


def load_dataset(
    name: str, df_format: Literal["polars", "pandas"] = "polars"
) -> nw.DataFrame:
    """
    Alias for download_dataset to sign the intent of loading a local dataset.
    """
    return download_dataset(name, df_format)
