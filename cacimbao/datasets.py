from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from pathlib import Path
from typing import Union

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

    @staticmethod
    @abstractmethod
    def prepare(*args, **kwargs) -> Union[pl.DataFrame | None]:
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

    @staticmethod
    def prepare(*args, **kwargs):
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

    @staticmethod
    def prepare(csv_dir: str):
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


class SalarioMinimoRealVigenteDataset(BaseDataset):
    """Dataset for real and current minimum wage in Brazil."""

    name: str = "salario_minimo_real_vigente"
    local: bool = True
    size: Size = Size.SMALL
    description: str = (
        "Salário mínimo real e vigente de 1940 a 2024."
        "Contém dados mensais do salário mínimo real (ajustado pela inflação) e o salário mínimo vigente (valor atual)."
        "Tem por volta de shape: 1.000 linhas e 3 colunas (valor pode mudar com a atualização da base)."
    )
    url: str = "http://www.ipeadata.gov.br/Default.aspx"
    filepath: Path = Path("salario-minimo/salario-minimo-real-vigente-04062025.parquet")

    @staticmethod
    def prepare(real_salary_filepath: str, current_salary_filepath: str):
        """Prepare the salary data by merging two datasets from IPEA and MTE.

        Downloaded from: http://www.ipeadata.gov.br/Default.aspx
        * Salário mínimo real (GAC12_SALMINRE12)
        * Salário mínimo vigente (MTE12_SALMIN12)
        """
        real = pl.read_csv(
            real_salary_filepath,
            separator=";",
            schema={
                "Data": pl.String,
                "Salário mínimo real - R$ (do último mês) - Instituto de Pesquisa Econômica": pl.String,
            },
            truncate_ragged_lines=True,
        )
        current = pl.read_csv(
            current_salary_filepath,
            separator=";",
            schema={
                "Data": pl.String,
                "Salário mínimo vigente - R$ - Ministério da Economia, Outras (Min. Economia/Outras) - MTE12_SALMIN12": pl.String,
            },
            truncate_ragged_lines=True,
        )
        combined_data = real.join(
            current, on="Data"
        )  # merged data based on the "Data" column
        combined_data = combined_data.with_columns(
            pl.col("Data").str.to_date(format="%Y.%m")
        )
        combined_data = combined_data.with_columns(
            pl.col(
                "Salário mínimo real - R$ (do último mês) - Instituto de Pesquisa Econômica"
            )
            .str.replace(",", ".")
            .cast(pl.Float64)
        )
        combined_data = combined_data.with_columns(
            pl.col(
                "Salário mínimo vigente - R$ - Ministério da Economia, Outras (Min. Economia/Outras) - MTE12_SALMIN12"
            )
            .str.replace(",", ".")
            .cast(pl.Float64)
        )
        combined_data.write_parquet(
            f"data/salario-minimo/salario-minimo-real-vigente-{today_label()}.parquet"
        )
        return combined_data


class AldeiasIndigenasDataset(BaseDataset):
    """Dataset for indigenous villages in Brazil."""

    name: str = "aldeias_indigenas"
    local: bool = True
    size: Size = Size.SMALL
    description: str = (
        "Dados geoespaciais sobre aldeias indígenas, aldeias e coordenações regionais, técnicas locais e "
        "mapas das terras indígenas fornecidos pela Coordenação de Geoprocessamento da FUNAI. "
        "Tem por volta de 4.300 linhas e 13 colunas (valor pode mudar com a atualização da base)."
    )
    # from: https://dados.gov.br/dados/conjuntos-dados/tabela-de-aldeias-indgenas
    url: str = "https://www.gov.br/funai/pt-br/acesso-a-informacao/dados-abertos/base-de-dados/Tabeladealdeias.ods"
    filepath: Path = Path("aldeias-indigenas/aldeias-indigenas-08062025.parquet")

    @staticmethod
    def prepare(filepath: str):
        """The ODS file is open in LibreOffice Calc and saved as a CSV file.
        It is not possible to read the ODS file directly with Polars due to an open issue:
        https://github.com/pola-rs/polars/issues/14053"""
        df = pl.read_csv(source=filepath)
        filepath = f"aldeias-indigenas/aldeias-indigenas-{today_label()}.parquet"
        df.write_parquet(files("cacimbao.data").joinpath(filepath))
        return df


def list_datasets(include_metadata=False) -> list:
    """
    List available datasets.

    Args:
        include_metadata: If True, returns metadata for each dataset.

    Returns:
        List of dataset names or a list of dictionaries with dataset metadata.
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
