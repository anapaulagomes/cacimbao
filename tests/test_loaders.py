import polars as pl
import pytest

from cacimbao import download_dataset, list_datasets, load_dataset


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
