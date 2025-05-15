import polars as pl

from cacimbao.datasets import download_dataset, list_datasets


class TestListDatasets:
    def test_list_datasets(self):
        assert list_datasets() == ["filmografia_brasileira"]


class TestDownloadDataset:
    def test_download_dataset(self):
        df = download_dataset("filmografia_brasileira")
        assert isinstance(df, pl.DataFrame)
