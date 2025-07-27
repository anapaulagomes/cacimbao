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

Para adicionar uma nova base de dados, você deve criar uma nova classe que herda de `BaseDataset`. Siga estes passos:

### 1. Preparar os arquivos originais

Use a pasta `tmp/` (crie se não existir) para colocar os arquivos originais da base de dados que deseja adicionar.

### 2. Criar a classe da base de dados

No módulo `cacimbao.datasets`, crie uma nova classe que herda de `BaseDataset`. Siga o padrão das bases de dados existentes:

```python
class MinhaNovaBaseDeDados(BaseDataset):
    """Base de dados para minha nova fonte de dados."""

    name: str = "minha_nova_base_de_dados"
    local: bool = True  # True se os dados ficam no repositório, False se são baixados
    size: Size = Size.SMALL  # SMALL, MEDIUM ou LARGE (pensando no número de linhas ou tamanho em MB)
    description: str = (
        "Descrição detalhada da base de dados. "
        "Inclua informações sobre o conteúdo, número aproximado de linhas e colunas, "
        "e qualquer transformação importante feita nos dados."
    )
    url: str = "URL original da fonte de dados"
    filepath: Path = Path("pasta-da-base/nome-do-arquivo-01012025.parquet")  # considerando que está dentro de cacimbao/data/
    download_url: str = ""  # Apenas para bases de dados não-locais

    @classmethod
    def prepare(cls, *args, **kwargs) -> pl.DataFrame:
        """Método que prepara os dados para uso.

        Para bases de dados locais, este método deve:
        1. Ler os arquivos originais
        2. Fazer as transformações necessárias
        3. Salvar o arquivo parquet usando cls.new_filepath()
        4. Criar o datapackage usando cls.create_datapackage_from_file()
        5. Retornar o DataFrame

        Para bases de dados não-locais, pode retornar None.
        """
        # Implementar a lógica de preparação dos dados
        pass
```

### 3. Implementar o método `prepare`

O método `prepare` deve:

- **Para bases de dados locais**: Processar os arquivos originais, fazer transformações necessárias, salvar como parquet e criar o datapackage
- **Para bases de dados não-locais**: Pode retornar `None` (os dados são baixados quando necessário)

### 4. Atributos importantes

- **`name`**: Nome da base de dados em snake_case
- **`local`**: `True` se os dados vão junto com o pacote, `False` se são baixados por demanda (podem estar no repositório mas serem baixados só quando o usuário solicita)
- **`size`**: `Size.SMALL`, `Size.MEDIUM` ou `Size.LARGE`
- **`description`**: Descrição detalhada da base de dados
- **`url`**: URL original da fonte de dados
- **`filepath`**: Caminho relativo para o arquivo parquet (apenas para bases de dados locais)
- **`download_url`**: URL para download (apenas para bases de dados não-locais)

### 5. Requisitos para todas as bases de dados

**Importante**: Todas as bases de dados, independentemente de serem locais ou não-locais, devem:

- **Ter um arquivo `datapackage.json`**: Descrevendo a estrutura e metadados dos dados
- **Estar em formato parquet**: Para melhor performance e compressão

**Para bases de dados não-locais**:

- **Arquivo .zip**: É esperado que o arquivo de download seja um arquivo `.zip` contendo o parquet e o `datapackage.json`
- **Exemplos**:
  - `PesquisaNacionalDeSaude2019Dataset`: Base de dados não-local que vive no repositório (arquivo `.zip` incluído)
  - `FilmografiaBrasileiraDataset`: Base de dados não-local que vive fora do repositório (baixado de URL externa)

### 6. Criando a base de dados

Quando estiver pronto para criar a base de dados, execute o método `prepare` da sua classe no terminal:

```python
# necessário apenas para base de dados local
df = MinhaNovaBaseDeDados.prepare("caminho/para/arquivo/original.csv")
```

Após executar o método `prepare`, atualize o atributo `filepath` na classe para indicar o caminho da versão mais recente da base de dados pronta para consumo.
Verifique o arquivo `datapackage.json` para garantir que o `path` tem apenas o nome do arquivo de dados e não o caminho inteiro (precisa ser relativo).

### 7. Métodos úteis da classe `BaseDataset`

- `cls.new_filepath()`: Gera o caminho para o novo arquivo parquet com data atual
- `cls.create_datapackage_from_file(filepath)`: Cria o `datapackage.json` automaticamente
- `cls.filename_prefix()`: Converte o nome da base de dados para formato de arquivo

### 8. Exemplos de implementação

Veja exemplos completos nas bases de dados existentes:
- `PescadoresEPescadorasProfissionaisDataset`: Base de dados local com múltiplos arquivos CSV
- `SalarioMinimoRealVigenteDataset`: Base de dados local com múltiplas fontes
- `FilmografiaBrasileiraDataset`: Base de dados não-local (baixado quando necessário)

## Como executar os testes

Para executar os testes, use o comando:

```bash
pytest

# ou

uv run pytest
```
