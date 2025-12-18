# /// script
# requires-python = ">=3.10"
# dependencies = [
#     "marimo",
#     "polars",
#     "numpy",
#     "pandas",
#     "plotly",
#     "cacimbao",
# ]
# ///

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full")


@app.cell
def _():
    import json
    from pathlib import Path

    import marimo as mo
    import plotly.express as px
    import polars as pl

    return Path, json, mo, pl, px


@app.cell
def _(px):
    # color-blind friendly palette configuration
    COLOR_SPECIES = {"Cão": "#0173B2", "Gato": "#DE8F05"}

    COLOR_SEX = {"Macho": "#029E73", "Fêmea": "#CC78BC"}

    COLOR_STATES = px.colors.qualitative.Safe
    return COLOR_SEX, COLOR_SPECIES, COLOR_STATES


@app.cell
def _(mo):
    mo.md(r"""
    # Começando com o Cacimbão!

    Primeiro, vamos listar quais são as bases de dados que estão disponíveis.

    > Esse notebook foi criado com [Marimo](https://docs.marimo.io/) e o DataFrame com [Polars](docs.pola.rs/). Você pode carregar a base de dados em outros formatos, basta passar o parâmetro `format` (por exemplo: `format="pandas"`) quando criar seu DataFrame.
    >
    > Os gráficos são feitos com [Plotly](plotly.com/python/).
    """)
    return


@app.cell
def _():
    import cacimbao

    cacimbao.list_datasets()
    return (cacimbao,)


@app.cell
def _(mo):
    mo.md(r"""
    ## Uma pista da base de dados de hoje:

    ![](https://external-content.duckduckgo.com/iu/?u=https%3A%2F%2Fs2.glbimg.com%2F5gwPFrlS-k05jWDU1G-JnJpyVXw%3D%2F1080x608%2Ftop%2Fsmart%2Fhttps%3A%2F%2Fi.s3.glbimg.com%2Fv1%2FAUTH_ee6202d7f3f346a7a5d7affb807d8893%2Finternal_photos%2Fbs%2F2021%2Fo%2FK%2FtR27LTQzSW0vCgbIeZGQ%2Ftv-colosso-001-fernando-quevedo-ag.-o-globo.jpg&f=1&nofb=1&ipt=755cae0776e091561405d9f9635d327dd36bdad1eebe6c070499a53180a54d5f)

    Isso mesmo, o [SinPatinhas](https://www.gov.br/mma/pt-br/composicao/sbio/dpda/programas-e-Projetos/sinpatinhas)! A ferramenta de cadastro para cães e gatos do governo federal é fundamental para promover políticas públicas de bem-estar animal eficazes e baseadas em resultados.

    Nessa base de dados, vamos conhecer melhor os pets do Brasil!
    """)
    return


@app.cell
def _(cacimbao):
    [
        dataset
        for dataset in cacimbao.list_datasets(include_metadata=True)
        if dataset["name"] == "sinpatinhas"
    ][0]
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Preparando dados geográficos

    Excelente! Podemos ver onde estão os gatos e cachorros no Brasil.
    Para isso, precisamos de um arquivo `geojson` para exibir as informações geolocalizadas quando carregarmos a nossa base de dados.

    Usamos esse geojson [aqui](https://raw.githubusercontent.com/codeforamerica/click_that_hood/master/public/data/brazil-states.geojson) e seguimos o [tutorial para criar um mapa do Brasil com Plotly](https://python.plainenglish.io/how-to-create-a-interative-map-using-plotly-express-geojson-to-brazil-in-python-fb5527ae38fc?gi=13be1873beeb) (_em inglês_).

    Antes de carregar os dados, vamos precisar também transformar a sigla do estado  no seu nome. Vamos usar o `geojson` que acabamos de baixar para ajudar nessa tarefa.
    """)
    return


@app.cell
def _(Path, json):
    brazil_states_geojson = json.loads(
        Path("notebooks/public/brazil-states.geojson").read_text()
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
    mo.md(r"""
    ## Carregando os dados

    Hora de carregar os  dados. Para isso você só precisa chamar `cacimbao.load_dataset("sinpatinhas")`.

    Na linha seguinte mapeamentos a sigla ao nome do estado, assim o mapa será exibido corretamente.
    """)
    return


@app.cell
def _(cacimbao, pl, state_abbreviation_name):
    df = cacimbao.load_dataset("sinpatinhas")
    df = df.with_columns(
        [
            pl.col("uf")
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
    mo.md(r"""
    # Análise Exploratória: Pets do Brasil

    Vamos começar entendendo como os pets cadastrados no SinPatinhas estão distribuídos pelo país e como foi a adesão ao programa ao longo do tempo.
    """)
    return


@app.cell
def _(COLOR_SPECIES, df, px):
    _fig = px.pie(
        df.group_by("especie").len(),
        values="len",
        names="especie",
        title="Gatos vs cachorros no Brasil",
        hole=0.4,
        color="especie",
        color_discrete_map=COLOR_SPECIES,
    )
    _fig
    return


@app.cell
def _(COLOR_SPECIES, df, px):
    _fig = px.line(
        df.group_by(["especie", "datacadastro"]).len().sort("datacadastro"),
        x="datacadastro",
        y="len",
        color="especie",
        color_discrete_map=COLOR_SPECIES,
        title="Evolução dos cadastros desde a criação do programa",
        labels={
            "datacadastro": "Data de cadastro",
            "len": "Quantidade",
            "especie": "Espécie",
        },
    )
    _fig.update_layout(hovermode="x unified")
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Distribuição geográfica

    Agora vamos visualizar como os pets estão distribuídos geograficamente pelo Brasil.
    Esses mapas nos ajudam a entender quais regiões têm maior adesão ao programa SinPatinhas.
    """)
    return


@app.cell
def _(brazil_states_geojson, df, px):
    _fig = px.choropleth(
        df.group_by(["especie", "Estado"]).len(),
        locations="Estado",
        geojson=brazil_states_geojson,
        color="len",
        facet_col="especie",
        color_continuous_scale="Blues",
        title="Distribuição de gatos e cachorros por estado em Números Absolutos",
        labels={"len": "Quantidade"},
    )
    _fig.update_geos(fitbounds="locations", visible=False)
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Pets vs Habitantes

    Um estado populoso, como o de São Paulo, naturalmente terá mais pets cadastrados.
    Vamos normalizar os dados utilizando a população do [Censo 2022 do IBGE](https://sidra.ibge.gov.br/pesquisa/censo-demografico/demografico-2022/inicial).

    Isso nos permite responder: **onde há mais pets em relação ao número de habitantes?**
    """)
    return


@app.cell
def _(brazil_states_geojson, df, pl, pop_by_state, px):
    pets_rate_by_state = (
        df.group_by(["especie", "Estado"])
        .len()
        .rename({"len": "quantidade_pets"})
        .join(pop_by_state, on="Estado", how="inner")
        .with_columns(
            [
                (pl.col("quantidade_pets") / pl.col("populacao") * 1000).alias(
                    "pets_per_1000_hab"
                )
            ]
        )
    )

    _fig = px.choropleth(
        pets_rate_by_state,
        geojson=brazil_states_geojson,
        color="pets_per_1000_hab",
        facet_col="especie",
        locations="Estado",
        color_continuous_scale="Blues",
        labels={"pets_per_1000_hab": "Pets por 1000 hab"},
        title="Taxa de Pets Cadastrados por 1000 Habitantes - Censo 2022",
    )
    _fig.update_geos(fitbounds="locations", visible=False)
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Quais cidades tem mais pets proporcionalmente?

    Descendo ao nível municipal, podemos identificar com mais precisão a relação entre população e quantidade de pets cadastrados.

    Primeiro, vamos ver quais municípios têm mais pets em números absolutos:
    """)
    return


@app.cell
def _(COLOR_STATES, df, px):
    _fig = px.bar(
        df.group_by(["no_municipio", "Estado"])
        .len()
        .sort("len", descending=True)
        .limit(10),
        x="no_municipio",
        y="len",
        color="Estado",
        color_discrete_sequence=COLOR_STATES,
        title="Top 10 municípios com mais pets em Números Absolutos",
        labels={
            "no_municipio": "Município",
            "len": "Quantidade de pets",
            "Estado": "Estado",
        },
    )
    _fig.update_layout(xaxis={"categoryorder": "total descending"})
    _fig
    return


@app.cell
def _(COLOR_STATES, pets_vs_pop, pl, px, state_abbreviation_name):
    top_pets_per_capita = (
        pets_vs_pop.group_by(["no_municipio", "uf"])
        .agg(
            [
                pl.col("quantidade_pets").sum().alias("total_pets"),
                pl.col("populacao").first(),
                pl.col("pets_per_1000_hab").sum().alias("total_pets_per_1000_hab"),
            ]
        )
        .with_columns(
            [
                pl.col("uf")
                .map_elements(
                    lambda uf: state_abbreviation_name.get(uf), return_dtype=pl.String
                )
                .alias("Estado")
            ]
        )
        .sort("total_pets_per_1000_hab", descending=True)
        .limit(10)
    )

    _fig = px.bar(
        top_pets_per_capita,
        x="no_municipio",
        y="total_pets_per_1000_hab",
        color="Estado",
        color_discrete_sequence=COLOR_STATES,
        title="Top 10 Municípios com Maior Taxa de Pets por 1000 Habitantes",
        labels={
            "no_municipio": "Município",
            "total_pets_per_1000_hab": "Pets por 1000 hab",
            "Estado": "Estado",
        },
        hover_data=["total_pets", "populacao"],
    )
    _fig.update_layout(xaxis={"categoryorder": "total descending"})
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    Agora, o gráfico scatter mostra a relação entre população e pets cadastrados, onde o tamanho das bolhas representa a taxa de pets por habitantes:
    """)
    return


@app.cell
def _(pl, state_abbreviation_name):
    census_pop = (
        pl.read_csv(
            "notebooks/public/tabela4714_populacao_residente_area_territorial_densidade_demografica.csv",
            separator=";",
            encoding="utf-8",
            skip_rows=3,
            schema_overrides={"2022": pl.Int32},
            ignore_errors=True,
        )
        .head(5570)
        .with_columns(
            [
                pl.col("Município").str.extract(r"\(([A-Z]{2})\)$", 1).alias("uf"),
                pl.col("Município")
                .str.replace(r" \([A-Z]{2}\)$", "")
                .alias("no_municipio"),
            ]
        )
        .rename({"2022": "populacao"})
    )

    pop_by_state = (
        census_pop.group_by("uf")
        .agg(pl.col("populacao").sum())
        .with_columns(
            [
                pl.col("uf")
                .map_elements(
                    lambda uf: state_abbreviation_name.get(uf), return_dtype=pl.String
                )
                .alias("Estado")
            ]
        )
    )
    return census_pop, pop_by_state


@app.cell
def _(COLOR_SPECIES, census_pop, df, pl, px):
    pets_per_municipality = (
        df.group_by(["no_municipio", "uf", "especie"])
        .len()
        .rename({"len": "quantidade_pets"})
    )

    pets_vs_pop = pets_per_municipality.join(
        census_pop.select(["no_municipio", "uf", "populacao"]),
        on=["no_municipio", "uf"],
        how="inner",
    ).with_columns(
        [
            (pl.col("quantidade_pets") / pl.col("populacao") * 1000).alias(
                "pets_per_1000_hab"
            )
        ]
    )

    _fig = px.scatter(
        pets_vs_pop.to_pandas(),
        x="populacao",
        y="quantidade_pets",
        color="especie",
        color_discrete_map=COLOR_SPECIES,
        size="pets_per_1000_hab",
        hover_data=["no_municipio", "uf", "pets_per_1000_hab"],
        title="Quantidade de pets vs população (Censo 2022)",
        labels={
            "populacao": "População",
            "quantidade_pets": "Quantidade de pets cadastrados",
            "especie": "Espécie",
            "pets_per_1000_hab": "Pets por 1000 hab",
        },
        log_x=True,
        log_y=True,
        opacity=0.6,
    )

    _fig.update_layout(height=700, showlegend=True, hovermode="closest")
    _fig
    return (pets_vs_pop,)


@app.cell
def _(mo):
    mo.md(r"""
    **Interpretação:** Note como os rankings são completamente diferentes! Os municípios com maior taxa de pets por habitante geralmente são cidades menores, enquanto os números absolutos são dominados por grandes centros urbanos. Isso pode indicar:
    - **Cidades menores**: Maior conscientização sobre a importância do cadastro ou programas municipais mais eficazes
    - **Grandes capitais**: Apesar de terem mais pets em números absolutos, a taxa per capita pode ser menor devido à subnotificação
    - **Fatores socioeconômicos**: Cidades com melhor infraestrutura de saúde animal tendem a ter mais cadastros
    """)
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## Características dos Pets Cadastrados

    Além de saber onde estão, é interessante conhecer as características dos pets brasileiros: sexo, cor da pelagem e faixa etária.

    ### Distribuição por Sexo
    """)
    return


@app.cell
def _(COLOR_SEX, df, px):
    _fig = px.bar(
        df.group_by(["sexo", "especie"]).len(),
        x="especie",
        y="len",
        color="sexo",
        color_discrete_map=COLOR_SEX,
        barmode="group",
        title="Distribuição de Sexo por Espécie",
        labels={"especie": "Espécie", "len": "Quantidade", "sexo": "Sexo"},
        text="len",
    )
    _fig.update_traces(texttemplate="%{text:.2s}", textposition="outside")
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Cor da Pelagem

    A diversidade de cores de pelagem varia entre as espécies. Vamos visualizar quais são as cores mais comuns entre gatos e cachorros.
    """)
    return


@app.cell
def _(COLOR_SEX, df, px):
    _fig = px.bar(
        df.group_by(["sexo", "corpelagem", "especie"]).len(),
        x="corpelagem",
        y="len",
        color="sexo",
        color_discrete_map=COLOR_SEX,
        facet_row="especie",
        title="Distribuição de sexo e cor de pelagem",
        labels={
            "corpelagem": "Cor da Pelagem",
            "len": "Quantidade",
            "sexo": "Sexo",
            "especie": "Espécie",
        },
    )
    _fig
    return


@app.cell
def _(COLOR_SPECIES, df, px):
    _fig = px.bar(
        df.group_by(["corpelagem", "especie"]).len().sort("len", descending=True),
        x="corpelagem",
        y="len",
        color="especie",
        color_discrete_map=COLOR_SPECIES,
        barmode="group",
        title="Distribuição de Cores de Pelagem por Espécie",
        labels={
            "corpelagem": "Cor da Pelagem",
            "len": "Quantidade",
            "especie": "Espécie",
        },
    )
    _fig.update_layout(xaxis={"categoryorder": "total descending"})
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Faixa Etária
    """)
    return


@app.cell
def _(COLOR_SPECIES, df, px):
    _fig = px.bar(
        df.group_by(["especie", "idade"]).len().sort("idade"),
        x="idade",
        y="len",
        color="especie",
        color_discrete_map=COLOR_SPECIES,
        barmode="group",
        title="Distribuição etária dos pets cadastrados",
        labels={"idade": "Faixa Etária", "len": "Quantidade", "especie": "Espécie"},
    )
    _fig
    return


@app.cell
def _(mo):
    mo.md(r"""
    ## O que mais podemos saber?

    Os dados revelam padrões interessantes nas preferências e características dos pets brasileiros.
    Há uma concentração de cadastros em determinadas regiões, o que pode indicar diferenças na implementação e divulgação do programa.
    Quando normalizamos pela população, descobrimos que municípios menores muitas vezes têm taxas maiores que grandes capitais

    Mas ainda dá pra saber mais:

    * Como a urbanização afeta a proporção entre gatos e cachorros?
    * Quais fatores influenciam a adesão ao programa SinPatinhas em diferentes regiões?
    * ... muito mais!

    ---

    **Explore os dados você mesmo!** Use o Cacimbão para carregar o dataset e fazer suas próprias análises:

    ```python
    import cacimbao
    df = cacimbao.load_dataset("sinpatinhas")
    ```

    Divirta-se explorando! 🐾
    """)
    return


if __name__ == "__main__":
    app.run()
