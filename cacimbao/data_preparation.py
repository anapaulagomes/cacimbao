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
        "data/salario-minimo/salario-minimo-real-vigente.parquet"
    )
    return combined_data
