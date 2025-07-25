from dataclasses import dataclass
from enum import Enum
from pathlib import Path


class Size(Enum):
    """Enum for dataset sizes."""

    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


@dataclass
class BaseDataset:
    """Base class for a dataset."""

    name: str
    size: Size
    description: str
    url: str  # original URL of the dataset
    local: bool
    filepath: Path = Path()
    download_url: str = ""


class FilmografiaBrasileiraDataset(BaseDataset):
    """Dataset for Brazilian filmography."""

    name: str = "filmografia_brasileira"
    local: bool = False
    size: Size = Size.MEDIUM
    description: str = (
        "Base de dados da filmografia brasileira produzido pela Cinemateca Brasileira. "
        "Contém informações sobre filmes e seus diretores, fontes, canções, atores e mais. "
        "Tem por volta de shape: 57.495 linhas e 37 colunas (valor pode mudar com a atualização da base)."
    )
    url: str = "https://bases.cinemateca.org.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis&base=FILMOGRAFIA&lang=p"
    download_url: str = "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-15052025.zip"


def list_datasets(include_metadata=False):
    """
    List available datasets.

    Args:
        include_metadata: If True, returns metadata for each dataset.

    Returns:
        List of dataset names or a dictionary with dataset metadata.
    """
    all_datasets = []
    for dataset in BaseDataset.__subclasses__():
        dataset_attributes = dataset.__dataclass_fields__.keys()
        if include_metadata:
            metadata = {
                key: value
                for key, value in dataset.__dict__.items()
                if key in dataset_attributes
            }
            all_datasets.append(metadata)
        else:
            all_datasets.append(dataset.name)
    return all_datasets


def get_dataset(name: str):
    """
    Get a dataset by name.

    Args:
        name: Name of the dataset.

    Returns:
        An instance of the dataset class.
    """
    for dataset in BaseDataset.__subclasses__():
        if dataset.name == name:
            return dataset
    raise ValueError(
        f"Base de dados '{name}' não encontrada. Use list_datasets() para ver as bases disponíveis."
    )
