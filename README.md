# cacimbão

<img src="cacimbao_logo.svg" alt="cacimbão logo" width="125" align="right"/>

[![PyPI - Version](https://img.shields.io/pypi/v/cacimbao)](https://pypi.org/project/cacimbao/) [![Test](https://github.com/anapaulagomes/cacimbao/actions/workflows/ci.yml/badge.svg)](https://github.com/anapaulagomes/cacimbao/actions/workflows/ci.yml) [![DOI](https://zenodo.org/badge/975927667.svg)](https://doi.org/10.5281/zenodo.15640773)

Bases de dados brasileiras para fins educacionais 🤓 [demo](https://huggingface.co/spaces/anapaulagomes/pescadores-e-pescadoras-profissionais)

## Uso

Primeiro, instale o pacote com o PyPi ou interaja através do notebook <a target="_blank" href="https://marimo.app/l/8pi68g"><img src="https://marimo.io/shield.svg"/></a>

```bash
pip install cacimbao
```

### Base de dados disponíveis

Depois, você pode usar o pacote para ver as bases disponíveis:

```python
import cacimbao

cacimbao.list_datasets()
```

Se quiser ver mais detalhes sobre as base de dados disponíveis, você pode usar:

```python
import cacimbao

cacimbao.list_datasets(include_metadata=True)
```

### Carregando uma base de dados local

Carregue um dataset local:

```python
df = cacimbao.load_dataset("pescadores_e_pescadoras_profissionais")
```

### Carregando uma base de dados remota

Ou escolha um dataset para _download_:

```python
df = cacimbao.download_dataset("filmografia_brasileira")
```

### Escolha do formato do dataframe

Você pode também escolher qual o formato do dataframe na sua biblioteca preferida.
A biblioteca padrão é o Polars mas você pode trabalhar com Pandas também.

```python
df = cacimbao.download_dataset("filmografia_brasileira", df_format="pandas")
```

## O que é um cacimbão?

Veja o que é um cacimbão [aqui](https://www.youtube.com/watch?v=Ft8-XXILjgE).
