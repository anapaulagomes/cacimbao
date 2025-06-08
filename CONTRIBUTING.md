# Como contribuir

Antes de começar, instale as dependências de desenvolvimento e análise de dados:

```bash
uv sync --group dev
uv sync --group analysis
```

Ative o ambiente virtual criado pelo `uv`:

```bash
source .venv/bin/activate
```

## Como adicionar uma nova base de dados

Use a pasta `tmp/` (crie se não existir) para colocar os arquivos originais da base de dados que deseja adicionar.

No módulo `cacimbao.data_preparation`, crie uma função que leia os arquivos da base de dados, escreva tudo em um arquivo
parquet único (assim ele fica menor) e retorne um `DataFrame` com as colunas necessárias. O arquivo parquet deve ser salvo
na pasta `cacimbao/data/<nome-da-base>`. O nome da base deve estar em letras minúsculas, separado por hífen e ser claro.
O nome do arquivo parquet deve seguir o padrão `<nome-da-base>-<diamesano>.parquet`, onde `<diamesano>`
é a data de criação do arquivo no formato `DDMMYYYY`.

Usando a biblioteca `frictionless`, crie um arquivo JSON para descrever a base de dados. O arquivo deve ser salvo
na mesma pasta que o arquivo parquet, com o nome `datapackage.json`. Gere esse arquivo automaticamente com o comando:

```bash
frictionless describe --json --format parquet data/<dataset>/<dataset>-<diamesano>.parquet > data/<dataset>/datapackage.json
```

Depois adicione os metadados da base de dados no módulo `cacimbao.datasets` na constante `DATASETS_METADATA`,
seguindo o padrão dos outros datasets.

## Como executar os testes

Para executar os testes, use o comando:

```bash
pytest

# ou

uv run pytest
```
