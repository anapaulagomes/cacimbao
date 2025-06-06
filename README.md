# cacimbao

Bases de dados brasileiras para fins educacionais

## Uso

Primeiro, instale o pacote com o PyPi:

```bash
pip install cacimbao
```

Depois, você pode usar o pacote para ver as bases disponíveis:

```python
import cacimbao

cacimbao.list_datasets()  # name, size, description, local or not
```

Carregue um dataset local:

```python
df = cacimbao.load_dataset("pescadores_e_pescadoras_profissionais")
```

Ou escolha um dataset para _download_:

```python
df = cacimbao.download_dataset("filmografia_brasileira")
```

Você pode também escolher qual o formato do dataframe na sua biblioteca preferida.
A biblioteca padrão é o Polars mas você pode trabalhar com Pandas também.

```python
df = cacimbao.download_dataset("filmografia_brasileira", df_format="pandas")
```
