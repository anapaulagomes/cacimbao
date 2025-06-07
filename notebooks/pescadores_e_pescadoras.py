# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo",
#     "cacimbao==0.1.2",
#     "polars==1.29.0",
#     "plotly[express]==6.1.2",
# ]
# ///
import marimo

__generated_with = "0.13.9"
app = marimo.App(width="columns")


@app.cell
def _():
    import json
    from pathlib import Path

    import marimo as mo
    import plotly.express as px
    import polars as pl

    return Path, json, mo, pl, px


@app.cell
def _(mo):
    mo.md(
        r"""
    # Começando com o Cacimbão!

    Primeiro, vamos listar quais são as bases de dados que estão disponíveis.

    > Esse notebook foi criado com [Marimo](https://docs.marimo.io/) e o DataFrame com [Polars](docs.pola.rs/). Você pode carregar a base de dados em outros formatos, basta passar o parâmetro `format` eg `format="pandas"` quando criar seu DataFrame. Os gráficos são feitos com [Plotly](plotly.com/python/).
    """
    )
    return


@app.cell
def _():
    import cacimbao

    cacimbao.list_datasets()
    return (cacimbao,)


@app.cell
def _(mo):
    mo.md(
        r"""A base de dados de pescadores e pescadoras parece interessante. Vamos procurar mais informações!"""
    )
    return


@app.cell
def _(cacimbao):
    cacimbao.list_datasets(include_metadata=True)[
        "pescadores_e_pescadoras_profissionais"
    ]
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Preparando dados geográficos

    Excelente! Podemos ver onde estão esses profissionais no Brasil.
    Para isso, precisamos de um arquivo `geojson` para exibir as informações geolocalizadas quando carregarmos a nossa base de dados.

    Usamos esse geojson [aqui](https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson) e seguimos o [tutorial para criar um mapa do Brasil com Plotly](https://python.plainenglish.io/how-to-create-a-interative-map-using-plotly-express-geojson-to-brazil-in-python-fb5527ae38fc?gi=13be1873beeb) (em inglês).

    Antes de carregar os dados, vamos precisar também transformar a sigla do estado  no seu nome. Vamos usar o `geojson` que acabamos de baixar para ajudar nessa tarefa.
    """
    )
    return


@app.cell
def _(Path, json):
    brazil_states_geojson = json.loads(
        Path("notebooks/brazil-states.geojson").read_text()
    )
    for feature in brazil_states_geojson["features"]:
        feature["id"] = feature["properties"]["name"]

    state_abbreviation_name = {
        state["properties"]["sigla"]: state["id"]
        for state in brazil_states_geojson["features"]
    }
    return brazil_states_geojson, state_abbreviation_name


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Carregando os dados

    Hora de carregar os  dados. Para isso você só precisa chamar `cacimbao.load_dataset("pescadores_e_pescadoras_profissionais")`.

    Na linha seguinte mapeamentos a sigla ao nome do estado, assim o mapa será exibido corretamente.
    """
    )
    return


@app.cell
def _(cacimbao, pl, state_abbreviation_name):
    df = cacimbao.load_dataset("pescadores_e_pescadoras_profissionais")
    df = df.with_columns(
        [
            pl.col("UF")
            .map_elements(
                lambda uf: state_abbreviation_name.get(uf), return_dtype=pl.String
            )
            .alias("Estado"),
        ]
    )
    df
    return (df,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ## Análise: Onde estão os pescadoras e pescadores profissionais no Brasil?

    Primeiro, vamos contar quantos profissionais existem em cada estado.
    """
    )
    return


@app.cell
def _(df):
    count_per_state = df.group_by("Estado").len()
    count_per_state
    return (count_per_state,)


@app.cell
def _(brazil_states_geojson, count_per_state, mo, px):
    _fig = px.choropleth(
        count_per_state,
        locations="Estado",
        geojson=brazil_states_geojson,
        color="len",
        title="Distribuição de pescadores e pescadoras profissionais por Estado (UF)",
    )
    _fig.update_geos(fitbounds="locations", visible=False)
    mo.ui.plotly(_fig)
    return


@app.cell
def _(df, mo, px):
    _fig = px.bar(
        df.group_by(["Municipio", "Estado"])
        .len()
        .sort("len", descending=True)
        .limit(10),
        x="Municipio",
        y="len",
        color="Estado",
        title="Top 10 cidades com mais pescadores e pescadoras no Brasil",
    )
    _fig.update_layout(barmode="stack", xaxis={"categoryorder": "total descending"})
    mo.ui.plotly(_fig)
    return


@app.cell
def _(mo):
    mo.md(r"""## Análise: Faixa de renda e gênero se relacionam?""")
    return


@app.cell
def _(df):
    df["Faixa de Renda"].unique()
    return


@app.cell
def _(df, mo, px):
    _fig = px.bar(
        df.group_by(["Faixa de Renda", "Sexo"]).len(),
        x="Faixa de Renda",
        y="len",
        color="Sexo",
        barmode="group",
        title="Faixa de renda e gênero",
        category_orders={
            "Faixa de Renda": [
                "Menor que R$1.045,00 por mês",
                "De R$1.045,00 a R$2.000,00",
                "De R$2.001,00 a R$3.000,00",
                "Acima de R$ 3.000,00",
            ]
        },
    )
    mo.ui.plotly(_fig)
    return


@app.cell
def _(mo):
    mo.md(
        r"""
    # Muitas outras perguntas para serem respondidas!

    * Como categoria e forma de atuação se relacionam? Existem estados que são mais especializados em tais formas?
    * A escolaridade influencia na faixa de renda desses profissionais?
    * A forma de atuação tem mais participação de um gênero ou é balanceado?

    Divirta-se!
    """
    )
    return


if __name__ == "__main__":
    app.run()
