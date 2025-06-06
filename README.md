# cacimbao

![PyPI - Version](https://img.shields.io/pypi/v/cacimbao) [![Test](https://github.com/anapaulagomes/cacimbao/actions/workflows/ci.yml/badge.svg)](https://github.com/anapaulagomes/cacimbao/actions/workflows/ci.yml)

Bases de dados brasileiras para fins educacionais

## Uso

Primeiro, instale o pacote com o PyPi:

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
