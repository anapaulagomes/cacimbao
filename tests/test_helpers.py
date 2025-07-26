import polars as pl
import pytest

from cacimbao.helpers import merge_csvs_to_parquet, normalize_column_name


class TestMergeCSVsToParquet:
    def test_merge_csvs_to_parquet(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv1 = data_dir / "file1.csv"
        csv1.write_text("col1,col2\n1,2\n3,4")
        csv2 = data_dir / "file2.csv"
        csv2.write_text("col1,col2\n5,6\n7,8")

        output_file = tmp_path / "merged.parquet"
        assert output_file.exists() is False

        merge_csvs_to_parquet(data_dir, str(output_file))

        assert output_file.exists() is True

        df = pl.read_parquet(output_file)
        assert df.shape == (4, 2)  # 4 rows and 2 columns

    def test_accept_arguments_to_read_csv(self, tmp_path):
        """Test that we can pass additional arguments to read_csv.

        In this case, the `truncate_ragged_lines` argument is used to ignore
        the extra columns in the second CSV file.
        """
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        csv1 = data_dir / "file1.csv"
        csv1.write_text("col1,col2\n1,2\n3,4")
        csv2 = data_dir / "file2.csv"
        csv2.write_text("col1,col2\n5,6\n7,8,9,10")

        output_file = tmp_path / "merged.parquet"
        assert output_file.exists() is False

        merge_csvs_to_parquet(data_dir, str(output_file), truncate_ragged_lines=True)

        assert output_file.exists() is True

        df = pl.read_parquet(output_file)
        assert df.shape == (4, 2)  # 4 rows and 2 columns


class TestNormalize:
    @pytest.mark.parametrize(
        "text,expected",
        [
            ("Unidade da Federação", "unidade_da_federacao"),
            (
                "Fazer uso de acupuntura, plantas medicinais e fitoterapia, homeopatia, "
                "meditação, yoga, tai chi chuan, liang gong ou alguma outra prática "
                "integrativa e complementar",
                "fazer_uso_de_acupuntura_plantas_medicinais_e_fitoterapia_homeopatia_"
                "meditacao_yoga_tai_chi_chuan_liang_gong_ou_alguma_outra_pratica_"
                "integrativa_e_complementar",
            ),
            (
                "O(A) Sr(a) pagou algum valor pelos medicamentos?",
                "o_a_sr_a_pagou_algum_valor_pelos_medicamentos",
            ),
            (
                "Peso - Final (em kg) (3 inteiros e 1 casa decimal)",
                "peso_final_em_kg_3_inteiros_e_1_casa_decimal",
            ),
            ("Quantos", "quantos"),
        ],
    )
    def test_normalize(self, text, expected):
        assert normalize_column_name(text) == expected
