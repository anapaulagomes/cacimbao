import polars as pl

from cacimbao.helpers import merge_csvs_to_parquet


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
