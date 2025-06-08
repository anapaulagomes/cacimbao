from datetime import date
from pathlib import Path

import polars as pl


def prepare_salario_minimo_data(
    real_salary_filepath: str, current_salary_filepath: str
) -> pl.DataFrame:
    """
    Prepare the salary data by merging two datasets from IPEA and MTE.

    Downloaded from: http://www.ipeadata.gov.br/Default.aspx
    * Salário mínimo real (GAC12_SALMINRE12)
    * Salário mínimo vigente (MTE12_SALMIN12)
    """
    real = pl.read_csv(
        real_salary_filepath,
        separator=";",
        schema={
            "Data": pl.String,
            "Salário mínimo real - R$ (do último mês) - Instituto de Pesquisa Econômica": pl.String,
        },
        truncate_ragged_lines=True,
    )
    current = pl.read_csv(
        current_salary_filepath,
        separator=";",
        schema={
            "Data": pl.String,
            "Salário mínimo vigente - R$ - Ministério da Economia, Outras (Min. Economia/Outras) - MTE12_SALMIN12": pl.String,
        },
        truncate_ragged_lines=True,
    )
    combined_data = real.join(
        current, on="Data"
    )  # merged data based on the "Data" column
    combined_data = combined_data.with_columns(
        pl.col("Data").str.to_date(format="%Y.%m")
    )
    combined_data = combined_data.with_columns(
        pl.col(
            "Salário mínimo real - R$ (do último mês) - Instituto de Pesquisa Econômica"
        )
        .str.replace(",", ".")
        .cast(pl.Float64)
    )
    combined_data = combined_data.with_columns(
        pl.col(
            "Salário mínimo vigente - R$ - Ministério da Economia, Outras (Min. Economia/Outras) - MTE12_SALMIN12"
        )
        .str.replace(",", ".")
        .cast(pl.Float64)
    )
    combined_data.write_parquet(
        f"data/salario-minimo/salario-minimo-real-vigente-{today_label()}.parquet"
    )
    return combined_data


def today_label() -> str:
    """Return today's date in the format DDMMYYYY."""
    return date.today().strftime("%d%m%Y")


def prepare_pescadores_data(csv_dir: str) -> pl.DataFrame:
    output_filepath = f"data/pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-{today_label()}.parquet"
    drop_columns = ["CPF", "Nome do Pescador"]  # personal information
    combined_data = merge_csvs_to_parquet(
        Path(csv_dir),
        output_filepath,
        drop_columns,
        separator=";",
        truncate_ragged_lines=True,
    )
    return combined_data


def merge_csvs_to_parquet(
    data_dir: Path, output_file: str, drop_columns=None, **read_csv_kwargs
):
    """Given a directory with csv files, merge them into a single parquet file."""
    data_dir_glob = f"{data_dir}/*.csv"
    df = pl.read_csv(data_dir_glob, **read_csv_kwargs)
    if drop_columns:
        df = df.drop(drop_columns)
    df.write_parquet(output_file)
    return output_file
