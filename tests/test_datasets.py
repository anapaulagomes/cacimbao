import os
from pathlib import Path

import pytest
from freezegun import freeze_time

from cacimbao.datasets import PesquisaNacionalDeSaude2019Dataset, Size, list_datasets

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
        "filepath": Path(
            "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet"
        ),
    },
    {
        "name": "salario_minimo_real_vigente",
        "size": Size.SMALL,
        "description": "Salário mínimo real e vigente de 1940 a 2024."
        "Contém dados mensais do salário mínimo real (ajustado pela inflação) e o salário mínimo vigente (valor atual)."
        "Tem por volta de shape: 1.000 linhas e 3 colunas (valor pode mudar com a atualização da base).",
        "url": "http://www.ipeadata.gov.br/Default.aspx",
        "local": True,
        "filepath": Path("salario-minimo/salario-minimo-real-vigente-04062025.parquet"),
    },
    {
        "name": "aldeias_indigenas",
        "size": Size.SMALL,
        "description": "Dados geoespaciais sobre aldeias indígenas, aldeias e coordenações regionais, técnicas locais e mapas das terras indígenas fornecidos pela Coordenação de Geoprocessamento da FUNAI. Tem por volta de 4.300 linhas e 13 colunas (valor pode mudar com a atualização da base).",
        "url": "https://www.gov.br/funai/pt-br/acesso-a-informacao/dados-abertos/base-de-dados/Tabeladealdeias.ods",
        "local": True,
        "filepath": Path("aldeias-indigenas/aldeias-indigenas-08062025.parquet"),
    },
    {
        "name": "pesquisa_nacional_de_saude_2019",
        "local": False,
        "size": Size.LARGE,
        "description": "Pesquisa Nacional de Saúde 2019, realizada pelo IBGE. Contém dados sobre condições de saúde, "
        "acesso e uso dos serviços de saúde, e outros aspectos relacionados à saúde "
        "da população brasileira. Tem por volta de 293.726 linhas e 1.087 colunas "
        "(valor pode mudar com a atualização da base).",
        "url": "https://www.pns.icict.fiocruz.br/bases-de-dados/",
        "download_url": "https://raw.githubusercontent.com/anapaulagomes/cacimbao/add-pns/cacimbao/data/pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-26072025.parquet.zip",
        "filepath": Path(
            "pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-25072025.parquet"
        ),
    },
]


class TestListDatasets:
    def test_list_datasets(self):
        expected_datasets = [
            "filmografia_brasileira",
            "pescadores_e_pescadoras_profissionais",
            "salario_minimo_real_vigente",
            "aldeias_indigenas",
            "pesquisa_nacional_de_saude_2019",
        ]
        assert list_datasets() == expected_datasets

    def test_list_datasets_with_metadata(self):
        datasets = list_datasets(include_metadata=True)
        assert isinstance(datasets, list)
        assert datasets == EXPECTED_DATASETS_METADATA  # FIXME replace with class tests


@pytest.mark.integration
class TestPesquisaNacionalDeSaude2019Dataset:
    @freeze_time("2000-01-01")
    def test_prepare(self):
        zip_filepath = "tests/fixtures/sample_pns2019.zip"
        df = PesquisaNacionalDeSaude2019Dataset.prepare(zip_filepath)

        assert df.shape == (99, 1087)
        os.unlink(PesquisaNacionalDeSaude2019Dataset.new_filepath())
        os.unlink(PesquisaNacionalDeSaude2019Dataset.new_datapackage_filepath())
