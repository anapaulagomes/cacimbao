from pathlib import Path

import polars as pl


def merge_csvs_to_parquet(data_dir: Path, output_file: str, **read_csv_kwargs):
    """Given a directory with csv files, merge them into a single parquet file."""
    data_dir_glob = f"{data_dir}/*.csv"
    pl.read_csv(data_dir_glob, **read_csv_kwargs).write_parquet(output_file)
    return output_file
