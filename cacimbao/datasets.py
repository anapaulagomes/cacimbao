import io
import json
import zipfile
from pathlib import Path
from typing import Dict, List, Literal, Union

import narwhals as nw
import requests

DATASETS_DIR = Path.home() / "cacimbao"
DATASETS_DIR.mkdir(parents=True, exist_ok=True)

DATASETS_METADATA: Dict[str, Dict] = {
    "filmografia_brasileira": {
        "name": "filmografia_brasileira",
        "size": "medium",  # small / medium / large  # TODO establish a standard for this
        "description": "Base de dados da filmografia brasileira produzido pela Cinemateca Brasileira. "
        "Contém informações sobre filmes e seus diretores, fontes, canções, atores e mais. "
        "Tem por volta de shape: 57.495 linhas e 37 colunas (valor pode mudar com a atualização da base).",
        "local": False,
        "url": "https://bases.cinemateca.org.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis&base=FILMOGRAFIA&lang=p",
        "download_url": "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-15052025.zip",
    },
    "pescadores_e_pescadoras_profissionais": {
        "name": "pescadores_e_pescadoras_profissionais",
        "size": "large",
        "description": "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
        "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização."
        "Tem por volta de shape: 1.700.000 linhas e 10 colunas (valor pode mudar com a atualização da base).",
        "url": "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais",
        "local": True,
        "filepath": "data/pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-15052025.parquet",
    },
    "salario_minimo": {
        "name": "salario_minimo_real_vigente",
        "size": "small",
        "description": "Salário mínimo real e vigente de 1940 a 2024."
        "Contém dados mensais do salário mínimo real (ajustado pela inflação) e o salário mínimo vigente (valor atual)."
        "Tem por volta de shape: 1.000 linhas e 3 colunas (valor pode mudar com a atualização da base).",
        "url": "http://www.ipeadata.gov.br/Default.aspx",
        "local": True,
        "filepath": "data/salario-minimo/salario-minimo-real-vigente-04062025.parquet",
    },
}


def _download_and_extract_zip(url: str, target_dir: Path) -> Path:
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


def _load_datapackage(datapackage_path: Path) -> Dict:
    """
    Load and parse a datapackage.json file.

    Args:
        datapackage_path: Path to the datapackage.json file

    Returns:
        Dictionary containing the datapackage metadata
    """
    with open(datapackage_path, "r", encoding="utf-8") as f:
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
                metadata.update(
                    {
                        "description": datapackage.get(
                            "description", metadata["description"]
                        ),
                        "size": f"{resource.get('bytes', 0) / 1024 / 1024:.1f}MB",
                        "filename": resource["path"],
                    }
                )

    return metadata


def list_datasets(include_metadata=False) -> Union[List[str], Dict[str, Dict]]:
    if include_metadata:
        return DATASETS_METADATA
    return list(DATASETS_METADATA.keys())


def download_dataset(name: str, df_format: Literal["polars", "pandas"] = "polars"):
    """
    Download and load a dataset.

    Args:
        name: Name of the dataset to download
        df_format: Format of the returned dataframe ("polars" or "pandas")

    Returns:
        DataFrame in the specified format
    """
    if name not in DATASETS_METADATA:
        raise ValueError(
            f"Base de dados '{name}' não encontrada. Use list_datasets() para ver as bases disponíveis."
        )

    dataset_info = DATASETS_METADATA[name]

    if dataset_info["local"]:
        file_path = Path(".").cwd() / dataset_info["filepath"]
        if not file_path.exists():
            raise FileNotFoundError(f"Local dataset '{name}' not found at {file_path}")
    else:
        file_path = DATASETS_DIR / name
        file_path = _download_and_extract_zip(dataset_info["download_url"], file_path)

        # load the datapackage.json to get the correct filename
        datapackage = _load_datapackage(file_path / "datapackage.json")
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


def load_dataset(name: str, df_format: Literal["polars", "pandas"] = "polars"):
    """
    Alias for download_dataset to sign the intent of loading a local dataset.
    """
    return download_dataset(name, df_format)
