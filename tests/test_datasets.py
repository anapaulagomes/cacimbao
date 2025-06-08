import polars as pl
import pytest

from cacimbao import download_dataset, list_datasets, load_dataset
from cacimbao.datasets import DATASETS_METADATA


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
        assert isinstance(datasets, dict)
        assert datasets == DATASETS_METADATA


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
            dataset
            for dataset, metadata in list_datasets(include_metadata=True).items()
            if metadata["local"]
        ],
    )
    def test_load_all_local_dataset(self, dataset_name):
        df = load_dataset(dataset_name)
        assert isinstance(df, pl.DataFrame)
        assert df.shape[0] > 0, f"Dataset {dataset_name} is empty"
