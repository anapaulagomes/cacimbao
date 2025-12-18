import logging
from abc import abstractmethod
from dataclasses import dataclass
from enum import Enum
from importlib.resources import files
from pathlib import Path
from typing import Union
from zipfile import ZipFile

import polars as pl

from cacimbao.helpers import merge_csvs_to_parquet, normalize_column_name, today_label

logger = logging.getLogger(__name__)


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

    @classmethod
    @abstractmethod
    def prepare(cls, *args, **kwargs) -> Union[pl.DataFrame | None]:
        """This method orchestrates the preparation steps of the dataset for use.

        This method should be implemented by subclasses that are local.
        It is expected to handle the preparation of the dataset, such as merging files,
        write to parquet, and any other necessary transformations, and return a Polars DataFrame."""

    @classmethod
    def filename_prefix(cls) -> str:
        return cls.name.replace("_", "-")

    @classmethod
    def dir(cls):
        return files("cacimbao.data").joinpath(cls.filename_prefix())

    @classmethod
    def datapackage_filepath(cls) -> str:
        return f"{cls.dir()}/datapackage.json"

    @classmethod
    def new_datapackage_filepath(cls) -> str:
        return f"{cls.dir()}/datapackage-{today_label()}.json"

    @classmethod
    def new_filepath(cls) -> str:
        filepath = f"{cls.dir()}/{cls.filename_prefix()}-{today_label()}.parquet"
        return str(files("cacimbao.data").joinpath(filepath))

    @classmethod
    def create_datapackage_from_file(cls, filepath):
        from frictionless import Resource

        resource = Resource(path=filepath)
        resource.infer()
        datapackage_filepath = cls.new_datapackage_filepath()
        resource.to_json(datapackage_filepath)
        return datapackage_filepath


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

    @classmethod
    def prepare(cls, *args, **kwargs):
        """Not local, so no preparation needed. The data is placed directly in the data folder."""


class PescadoresEPescadorasProfissionaisDataset(BaseDataset):
    """Dataset for professional fishermen and fisherwomen in Brazil."""

    name: str = "pescadores_e_pescadoras_profissionais"
    local: bool = True
    size: Size = Size.LARGE
    description: str = (
        "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
        "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização. "
        "Tem por volta de 1.700.000 linhas e 8 colunas (valor pode mudar com a atualização da base). "
        "A base de dados original tem 10 colunas. Duas colunas foram removidas: "
        "CPF e Nome do Pescador, por serem informações pessoais."
    )
    url: str = "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais"
    filepath: Path = Path(
        "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet"
    )

    @classmethod
    def prepare(cls, csv_dir: str):
        """Merge the CSVs from the states into one parquet file and remove personal information."""
        output_filepath = cls.new_filepath()
        drop_columns = ["CPF", "Nome do Pescador"]  # personal information
        merge_csvs_to_parquet(
            Path(csv_dir),
            output_filepath,
            drop_columns,
            separator=";",
            truncate_ragged_lines=True,
        )

        cls.create_datapackage_from_file(output_filepath)
        return pl.read_parquet(output_filepath)


class SalarioMinimoRealVigenteDataset(BaseDataset):
    """Dataset for real and current minimum wage in Brazil."""

    name: str = "salario_minimo_real_vigente"
    local: bool = True
    size: Size = Size.SMALL
    description: str = (
        "Salário mínimo real e vigente de 1940 a 2024. Contém dados mensais do "
        "salário mínimo real (ajustado pela inflação) e o salário mínimo vigente "
        "(valor atual). Tem por volta de 1.000 linhas e 3 colunas (valor pode "
        "mudar com a atualização da base)."
    )
    url: str = "http://www.ipeadata.gov.br/Default.aspx"
    filepath: Path = Path(
        "salario-minimo-real-vigente/salario-minimo-real-vigente-04062025.parquet"
    )

    @classmethod
    def prepare(cls, real_salary_filepath: str, current_salary_filepath: str):
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
        combined_data.write_parquet(cls.new_filepath())

        cls.create_datapackage_from_file(cls.new_filepath())
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

    @classmethod
    def prepare(cls, filepath: str):
        """The ODS file is open in LibreOffice Calc and saved as a CSV file.
        It is not possible to read the ODS file directly with Polars due to an open issue:
        https://github.com/pola-rs/polars/issues/14053"""
        df = pl.read_csv(source=filepath)
        filepath = cls.new_filepath()
        df.write_parquet(filepath)
        cls.create_datapackage_from_file(filepath)
        return df


class PesquisaNacionalDeSaude2019Dataset(BaseDataset):
    """Dataset for the 2019 National Health Survey in Brazil.

    This dataset is special because it lives in our repository, but it is downloaded only
    when required by the user. Since it is a large dataset, we do not want to have a large
    package size by default.

    The original dataset is available at:
        https://www.pns.icict.fiocruz.br/wp-content/uploads/2023/11/pns2019.zip
    """

    name: str = "pesquisa_nacional_de_saude_2019"
    local: bool = False
    size: Size = Size.LARGE  # currently, 33 MB
    description: str = (
        "Pesquisa Nacional de Saúde 2019, realizada pelo IBGE. "
        "Contém dados sobre condições de saúde, acesso e uso dos serviços de saúde, "
        "e outros aspectos relacionados à saúde da população brasileira. "
        "Tem por volta de 293.726 linhas e 1.087 colunas (valor pode mudar com a atualização da base)."
    )
    url: str = "https://www.pns.icict.fiocruz.br/bases-de-dados/"
    download_url: str = "https://raw.githubusercontent.com/anapaulagomes/cacimbao/main/cacimbao/data/pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-26072025.parquet.zip"
    filepath: Path = Path(
        "pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-25072025.parquet"
    )

    @classmethod
    def prepare(cls, zip_filepath: str) -> pl.DataFrame:
        logger.info("Preparando o dicionário de dados...")
        data_dict = cls._data_dict()
        logger.info("Hora descompactar o arquivo .zip e criar o .parquet...")
        parquet_filepath = cls._create_parquet_file(zip_filepath, data_dict)
        logger.info("Momento de criação do datapackage...")
        cls._create_datapackage(parquet_filepath, data_dict)
        logger.info("Fim.")

        return pl.read_parquet(parquet_filepath)

    @classmethod
    def _create_parquet_file(cls, zip_filepath: str, data_dict: dict) -> str:
        index = zip_filepath.rfind("/")
        csv_filename = zip_filepath[index + 1 :].replace(".zip", ".csv")
        df = pl.read_csv(ZipFile(zip_filepath).read(csv_filename))
        df.columns = [field["alternative_name"] for field in data_dict.values()]

        df.write_parquet(cls.new_filepath())
        return cls.new_filepath()

    @classmethod
    def _create_datapackage(cls, parquet_filepath: str, data_dict: dict):
        from frictionless import Resource, Schema

        resource = Resource(path=parquet_filepath, format="parquet")
        resource.infer()

        schema_descriptor = resource.schema.to_descriptor()

        modified_fields = []
        for field in schema_descriptor["fields"]:
            modified_field = field.copy()
            if data_dict.get(field["name"]):
                field_data = data_dict[field["name"]]
                modified_field["name"] = field_data["alternative_name"]
                modified_field["description"] = field_data["description"]
                modified_field["constraints"] = {
                    "enum": list(field_data["categories"].keys())
                }
            modified_fields.append(modified_field)

        schema_descriptor["fields"] = modified_fields
        # the path needs to be adjusted to the relative path so it can work when the user
        # downloads the dataset
        resource.path = parquet_filepath[parquet_filepath.rfind("/") + 1 :]
        resource.schema = Schema.from_descriptor(schema_descriptor)
        resource.to_json(cls.new_datapackage_filepath())
        return cls.new_datapackage_filepath()

    @classmethod
    def _data_dict(cls):
        data_dict_file = pl.read_excel(
            f"{cls.dir()}/dicionario_PNS_microdados_2019_23062023.xls",
            read_options={"header_row": 3},
        )
        data_dict_file.columns = [
            "posicao_inicial",
            "tamanho",
            "codigo_da_variavel",
            "quesito_nr",
            "quesito_descricao",
            "categorias_tipo",
            "categorias_descricao",
        ]

        section = ""
        data_dict = {}
        categories = {}
        current_field = {}
        for row in data_dict_file.iter_rows(named=True):
            number_of_nones = list(row.values()).count(None)
            if (
                row["posicao_inicial"] and number_of_nones == 6
            ):  # início de uma nova seção
                section = row["posicao_inicial"].strip()
            elif row["codigo_da_variavel"]:  # nova variável
                if current_field:
                    current_field["categories"] = categories
                    current_field["description"] += (
                        f" Opções: {categories}" if categories else ""
                    )
                    data_dict[current_field["name"]] = current_field
                    categories = {}

                normalized_question = normalize_column_name(row["quesito_descricao"])
                current_field = {
                    "name": row["codigo_da_variavel"],
                    "alternative_name": f"{row['codigo_da_variavel']}__{normalized_question}",
                    "description": f"{row['quesito_descricao']} (seção: {section}). Tamanho: {row['tamanho']}.",
                }
            else:
                categories[row["categorias_tipo"]] = row["categorias_descricao"]
        return data_dict


class SinPatinhasDataset(BaseDataset):
    """Dataset for animals from the SinPatinhas system."""

    name: str = "sinpatinhas"
    local: bool = True
    size: Size = Size.SMALL
    description: str = (
        "Animais do sistema SinPatinhas de 15 de abril a 2 de  dezembro."
        "Contém dados como espécie, idade, sexo, cor da pelagem, data de cadastro, "
        "e localização (UF e município). "
        "Tem por volta de 930.000 linhas e 7 colunas (valor pode mudar com a "
        "atualização da base)."
        "Os dados foram repassados a partir do recurso em 2ª instância, "
        "no pedido de informação SIC n. 02303.016805/2025 e publicados no DESPACHO "
        "Nº 98764/2025-MMA pela Sra. Ministra de Estado do Meio Ambiente (Marina Silva), "
        'que determinou a "disponibilização das informações identificadas como não sensíveis".'
    )
    url: str = "https://buscalai.cgu.gov.br/PedidosLai/DetalhePedido?id=9499381"
    filepath: Path = Path("sinpatinhas/sinpatinhas-09122025.parquet")

    @classmethod
    def prepare(cls, csv_filepath: str):
        """Read unzipped csv filepath and convert it to a parquet file."""
        output_filepath = cls.new_filepath()
        df = pl.read_csv(
            csv_filepath,
            separator=";",
            encoding="utf-8",
        ).with_columns(
            pl.col("datacadastro").str.to_date(format="%d/%m/%Y"),
        )
        df.write_parquet(output_filepath)
        cls.create_datapackage_from_file(output_filepath)
        return pl.read_parquet(output_filepath)


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
