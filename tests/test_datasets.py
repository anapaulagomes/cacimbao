import polars as pl
import pytest

from cacimbao import download_dataset, load_dataset
from cacimbao.new_datasets import Size, list_datasets

EXPECTED_DATASETS_METADATA = [
    {
        "name": "filmografia_brasileira",
        "size": Size.MEDIUM,
        "description": "Base de dados da filmografia brasileira produzido pela Cinemateca Brasileira. "
        "Contém informações sobre filmes e seus diretores, fontes, canções, atores e mais. "
        "Tem por volta de shape: 57.495 linhas e 37 colunas (valor pode mudar com a atualização da base).",
        "local": False,
        "url": "https://bases.cinemateca.org.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis&base=FILMOGRAFIA&lang=p",
        "download_url": "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-15052025.zip",
    },
    {
        "name": "pescadores_e_pescadoras_profissionais",
        "size": Size.LARGE,
        "description": "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
        "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização."
        "Tem por volta de shape: 1.700.000 linhas e 10 colunas (valor pode mudar com a atualização da base).",
        "url": "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais",
        "local": True,
        "filepath": "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet",
    },
    {
        "name": "salario_minimo_real_vigente",
        "size": Size.SMALL,
        "description": "Salário mínimo real e vigente de 1940 a 2024."
        "Contém dados mensais do salário mínimo real (ajustado pela inflação) e o salário mínimo vigente (valor atual)."
        "Tem por volta de shape: 1.000 linhas e 3 colunas (valor pode mudar com a atualização da base).",
        "url": "http://www.ipeadata.gov.br/Default.aspx",
        "local": True,
        "filepath": "salario-minimo/salario-minimo-real-vigente-04062025.parquet",
    },
    {
        "name": "aldeias_indigenas",
        "size": Size.SMALL,
        "description": "Dados geoespaciais sobre aldeias indígenas, aldeias e coordenações regionais, técnicas locais e mapas das terras indígenas fornecidos pela Coordenação de Geoprocessamento da FUNAI. Tem por volta de 4.300 linhas e 13 colunas (valor pode mudar com a atualização da base).",
        "url": "https://dados.gov.br/dados/conjuntos-dados/tabela-de-aldeias-indgenas",
        "local": True,
        "filepath": "aldeias-indigenas/aldeias-indigenas-08062025.parquet",
    },
]


class TestListDatasets:
    def test_list_datasets(self):
        expected_datasets = [
            "filmografia_brasileira",
            "pescadores_e_pescadoras_profissionais",
            "salario_minimo",
            "aldeias_indigenas",
        ]
        assert list_datasets() == expected_datasets

    def test_list_datasets_with_metadata(self):
        datasets = list_datasets(include_metadata=True)
        assert isinstance(datasets, list)
        assert datasets == EXPECTED_DATASETS_METADATA


class TestDownloadDataset:
    def test_download_dataset(self):
        df = download_dataset("filmografia_brasileira")
        assert isinstance(df, pl.DataFrame)

    def test_load_local_dataset(self):
        df = load_dataset("pescadores_e_pescadoras_profissionais")
        assert isinstance(df, pl.DataFrame)

    @pytest.mark.parametrize(
        "dataset_name",
        [
            dataset["name"]
            for dataset in list_datasets(include_metadata=True)
            if dataset["local"]
        ],
    )
    def test_load_all_local_dataset(self, dataset_name):
        df = load_dataset(dataset_name)
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] > 0, f"Dataset {dataset_name} is empty"
