import os
from pathlib import Path

import pytest
from freezegun import freeze_time

from cacimbao.datasets import (
    AldeiasIndigenasDataset,
    FilmografiaBrasileiraDataset,
    PescadoresEPescadorasProfissionaisDataset,
    PesquisaNacionalDeSaude2019Dataset,
    SalarioMinimoRealVigenteDataset,
    SinPatinhasDataset,
    Size,
    list_datasets,
)


class TestListDatasets:
    def test_list_datasets(self):
        expected_datasets = [
            "filmografia_brasileira",
            "pescadores_e_pescadoras_profissionais",
            "salario_minimo_real_vigente",
            "aldeias_indigenas",
            "pesquisa_nacional_de_saude_2019",
            "sinpatinhas",
        ]
        assert list_datasets() == expected_datasets


class TestPesquisaNacionalDeSaude2019Dataset:
    @pytest.mark.integration
    @freeze_time("2000-01-01")
    def test_prepare(self):
        zip_filepath = "tests/fixtures/sample_pns2019.zip"
        df = PesquisaNacionalDeSaude2019Dataset.prepare(zip_filepath)

        assert df.shape == (99, 1087)
        assert os.path.exists(PesquisaNacionalDeSaude2019Dataset.new_filepath())
        assert os.path.exists(
            PesquisaNacionalDeSaude2019Dataset.new_datapackage_filepath()
        )

        os.unlink(PesquisaNacionalDeSaude2019Dataset.new_filepath())
        os.unlink(PesquisaNacionalDeSaude2019Dataset.new_datapackage_filepath())

    def test_dataset_attributes(self):
        description = (
            "Pesquisa Nacional de Saúde 2019, realizada pelo IBGE. "
            "Contém dados sobre condições de saúde, acesso e uso dos serviços de saúde, "
            "e outros aspectos relacionados à saúde da população brasileira. "
            "Tem por volta de 293.726 linhas e 1.087 colunas (valor pode mudar com a atualização da base)."
        )
        url = "https://www.pns.icict.fiocruz.br/bases-de-dados/"
        download_url = "https://raw.githubusercontent.com/anapaulagomes/cacimbao/main/cacimbao/data/pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-26072025.parquet.zip"
        filepath = Path(
            "pesquisa-nacional-de-saude-2019/pesquisa-nacional-de-saude-2019-25072025.parquet"
        )
        assert (
            PesquisaNacionalDeSaude2019Dataset.name == "pesquisa_nacional_de_saude_2019"
        )
        assert PesquisaNacionalDeSaude2019Dataset.local is False
        assert PesquisaNacionalDeSaude2019Dataset.size == Size.LARGE
        assert PesquisaNacionalDeSaude2019Dataset.description == description
        assert PesquisaNacionalDeSaude2019Dataset.url == url
        assert PesquisaNacionalDeSaude2019Dataset.download_url == download_url
        assert PesquisaNacionalDeSaude2019Dataset.filepath == filepath


class TestFilmografiaBrasileiraDataset:
    @pytest.mark.integration
    def test_prepare(self):
        result = FilmografiaBrasileiraDataset.prepare()
        assert result is None

    def test_dataset_attributes(self):
        url = "https://bases.cinemateca.org.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis&base=FILMOGRAFIA&lang=p"
        download_url = "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-15052025.zip"
        description = (
            "Base de dados da filmografia brasileira produzido pela Cinemateca Brasileira. "
            "Contém informações sobre filmes e seus diretores, fontes, canções, atores e mais. "
            "Tem por volta de shape: 57.495 linhas e 37 colunas (valor pode mudar com a atualização da base)."
        )

        assert FilmografiaBrasileiraDataset.name == "filmografia_brasileira"
        assert FilmografiaBrasileiraDataset.local is False
        assert FilmografiaBrasileiraDataset.size == Size.MEDIUM
        assert FilmografiaBrasileiraDataset.description == description
        assert FilmografiaBrasileiraDataset.url == url
        assert FilmografiaBrasileiraDataset.download_url == download_url


class TestPescadoresEPescadorasProfissionaisDataset:
    @pytest.mark.integration
    @freeze_time("2000-01-01")
    def test_prepare(self):
        result = PescadoresEPescadorasProfissionaisDataset.prepare(
            "tests/fixtures/pescadores"
        )

        assert result.shape == (
            198,
            8,
        )  # 99 rows of each sample file (2 files in total)
        assert os.path.exists(PescadoresEPescadorasProfissionaisDataset.new_filepath())
        assert os.path.exists(
            PescadoresEPescadorasProfissionaisDataset.new_datapackage_filepath()
        )

        os.unlink(PescadoresEPescadorasProfissionaisDataset.new_filepath())
        os.unlink(PescadoresEPescadorasProfissionaisDataset.new_datapackage_filepath())

    def test_dataset_attributes(self):
        description = (
            "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
            "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização. "
            "Tem por volta de 1.700.000 linhas e 8 colunas (valor pode mudar com a atualização da base). "
            "A base de dados original tem 10 colunas. Duas colunas foram removidas: CPF e Nome do "
            "Pescador, por serem informações pessoais."
        )
        url = "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais"
        filepath = Path(
            "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet"
        )
        assert (
            PescadoresEPescadorasProfissionaisDataset.name
            == "pescadores_e_pescadoras_profissionais"
        )
        assert PescadoresEPescadorasProfissionaisDataset.local is True
        assert PescadoresEPescadorasProfissionaisDataset.size == Size.LARGE
        assert PescadoresEPescadorasProfissionaisDataset.description == description
        assert PescadoresEPescadorasProfissionaisDataset.url == url
        assert PescadoresEPescadorasProfissionaisDataset.filepath == filepath


class TestSalarioMinimoRealVigenteDataset:
    @pytest.mark.integration
    @freeze_time("2000-01-01")
    def test_prepare(self):
        real_salary_file = "tests/fixtures/salarios/ipeadata_GAC12_SALMINRE12.csv"
        current_salary_file = "tests/fixtures/salarios/ipeadata_MTE12_SALMIN12.csv"
        result = SalarioMinimoRealVigenteDataset.prepare(
            real_salary_file, current_salary_file
        )

        assert result.shape == (1020, 3)

        assert os.path.exists(SalarioMinimoRealVigenteDataset.new_filepath())
        assert os.path.exists(
            SalarioMinimoRealVigenteDataset.new_datapackage_filepath()
        )

        os.unlink(SalarioMinimoRealVigenteDataset.new_filepath())
        os.unlink(SalarioMinimoRealVigenteDataset.new_datapackage_filepath())

    def test_dataset_attributes(self):
        description = (
            "Salário mínimo real e vigente de 1940 a 2024. Contém dados mensais do "
            "salário mínimo real (ajustado pela inflação) e o salário mínimo vigente "
            "(valor atual). Tem por volta de 1.000 linhas e 3 colunas (valor pode "
            "mudar com a atualização da base)."
        )
        url = "http://www.ipeadata.gov.br/Default.aspx"
        filepath = Path(
            "salario-minimo-real-vigente/salario-minimo-real-vigente-04062025.parquet"
        )
        assert SalarioMinimoRealVigenteDataset.name == "salario_minimo_real_vigente"
        assert SalarioMinimoRealVigenteDataset.local is True
        assert SalarioMinimoRealVigenteDataset.size == Size.SMALL
        assert SalarioMinimoRealVigenteDataset.description == description
        assert SalarioMinimoRealVigenteDataset.url == url
        assert SalarioMinimoRealVigenteDataset.filepath == filepath


class TestAldeiasIndigenasDataset:
    @pytest.mark.integration
    @freeze_time("2000-01-01")
    def test_prepare(self):
        csv_file = "tests/fixtures/sample_aldeias.csv"

        result = AldeiasIndigenasDataset.prepare(csv_file)

        assert result.shape == (9, 13)
        assert os.path.exists(AldeiasIndigenasDataset.new_filepath())
        assert os.path.exists(AldeiasIndigenasDataset.new_datapackage_filepath())

        os.unlink(AldeiasIndigenasDataset.new_filepath())
        os.unlink(AldeiasIndigenasDataset.new_datapackage_filepath())

    def test_dataset_attributes(self):
        description = (
            "Dados geoespaciais sobre aldeias indígenas, aldeias e coordenações regionais, técnicas locais "
            "e mapas das terras indígenas fornecidos pela Coordenação de Geoprocessamento da FUNAI. "
            "Tem por volta de 4.300 linhas e 13 colunas (valor pode mudar com a atualização da base)."
        )
        url = "https://www.gov.br/funai/pt-br/acesso-a-informacao/dados-abertos/base-de-dados/Tabeladealdeias.ods"
        filepath = Path("aldeias-indigenas/aldeias-indigenas-08062025.parquet")
        assert AldeiasIndigenasDataset.name == "aldeias_indigenas"
        assert AldeiasIndigenasDataset.local is True
        assert AldeiasIndigenasDataset.size == Size.SMALL
        assert AldeiasIndigenasDataset.description == description
        assert AldeiasIndigenasDataset.url == url
        assert AldeiasIndigenasDataset.filepath == filepath


class TestSinPatinhas:
    @freeze_time("2000-01-01")
    def test_prepare(self, tmp_path):
        csv_content = """especie;idade;sexo;corpelagem;datacadastro;uf;no_municipio
        Gato;1;Fêmea;Outros;04/06/2025;SP;Osasco
        Cão;4;Macho;Tricolor;28/05/2025;PI;Parnaíba
        """
        csv_file = tmp_path / "sinpatinhas.csv"
        csv_file.write_text(csv_content, encoding="utf-8")

        result = SinPatinhasDataset.prepare(csv_file)

        assert result.shape == (3, 7)
        assert os.path.exists(SinPatinhasDataset.new_filepath())
        assert os.path.exists(SinPatinhasDataset.new_datapackage_filepath())

        os.unlink(SinPatinhasDataset.new_filepath())
        os.unlink(SinPatinhasDataset.new_datapackage_filepath())

    def test_dataset_attributes(self):
        description = (
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
        url = "https://buscalai.cgu.gov.br/PedidosLai/DetalhePedido?id=9499381"
        filepath = Path("sinpatinhas/sinpatinhas-09122025.parquet")
        assert SinPatinhasDataset.name == "sinpatinhas"
        assert SinPatinhasDataset.local is True
        assert SinPatinhasDataset.size == Size.SMALL
        assert SinPatinhasDataset.description == description
        assert SinPatinhasDataset.url == url
        assert SinPatinhasDataset.filepath == filepath
