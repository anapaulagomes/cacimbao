import polars as pl

from cacimbao import download_dataset, list_datasets, load_dataset
from cacimbao.datasets import DATASETS_METADATA


class TestListDatasets:
    def test_list_datasets(self):
        expected_datasets = [
            "filmografia_brasileira",
            "pescadores_e_pescadoras_profissionais",
            "salario_minimo",
        ]
        assert list_datasets() == expected_datasets

    def test_list_datasets_with_metadata(self):
        datasets = list_datasets(include_metadata=True)
        assert isinstance(datasets, dict)
        assert datasets == DATASETS_METADATA


class TestDownloadDataset:
    def test_download_dataset(self):
        df = download_dataset("filmografia_brasileira")
        assert isinstance(df, pl.DataFrame)

    def test_load_local_dataset(self):
        df = load_dataset("pescadores_e_pescadoras_profissionais")
        assert isinstance(df, pl.DataFrame)
