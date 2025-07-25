from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import polars as pl

from cacimbao.helpers import merge_csvs_to_parquet, today_label


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

    @abstractmethod
    def prepare(self, *args, **kwargs) -> pl.DataFrame:
        """This method orchestrates the preparation steps of the dataset for use.

        This method should be implemented by subclasses that are local.
        It is expected to handle the preparation of the dataset, such as merging files,
        write to parquet, and any other necessary transformations, and return a Polars DataFrame."""


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

    def prepare(self):
        """Not local, so no preparation needed. The data is placed directly in the data folder."""


class PescadoresEPescadorasProfissionaisDataset(BaseDataset):
    """Dataset for professional fishermen and fisherwomen in Brazil."""

    name: str = "pescadores_e_pescadoras_profissionais"
    local: bool = True
    size: Size = Size.LARGE
    description: str = (
        "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
        "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização."
        "Tem por volta de shape: 1.700.000 linhas e 10 colunas (valor pode mudar com a atualização da base)."
    )
    url: str = "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais"
    filepath: Path = Path(
        "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet"
    )

    def prepare(self, csv_dir):
        """Merge the CSVs from the states into one parquet file and remove personal information."""
        output_filepath = f"data/pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-{today_label()}.parquet"
        drop_columns = ["CPF", "Nome do Pescador"]  # personal information
        combined_data = merge_csvs_to_parquet(
            Path(csv_dir),
            output_filepath,
            drop_columns,
            separator=";",
            truncate_ragged_lines=True,
        )
        return combined_data


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
