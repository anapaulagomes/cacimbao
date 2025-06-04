import polars as pl

from cacimbao.datasets import download_dataset, list_datasets


class TestListDatasets:
    def test_list_datasets(self):
        expected_datasets = [
            "filmografia_brasileira",
            "pescadores_e_pescadoras_profissionais",
            "salario_minimo",
        ]
        assert list_datasets() == expected_datasets


class TestDownloadDataset:
    def test_download_dataset(self):
        df = download_dataset("filmografia_brasileira")
        assert isinstance(df, pl.DataFrame)

    def test_load_local_dataset(self):
        df = download_dataset("pescadores_e_pescadoras_profissionais")
        assert isinstance(df, pl.DataFrame)
