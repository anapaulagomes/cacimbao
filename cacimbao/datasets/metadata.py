from typing import Dict, List, Union

from ..helpers import DATASETS_DIR, load_datapackage

DATASETS_METADATA: Dict[str, Dict] = {
    "filmografia_brasileira": {
        "name": "filmografia_brasileira",
        "size": "medium",  # small / medium / large  # TODO establish a standard for this
        "description": "Base de dados da filmografia brasileira produzido pela Cinemateca Brasileira. "
        "Contém informações sobre filmes e seus diretores, fontes, canções, atores e mais. "
        "Tem por volta de shape: 57.495 linhas e 37 colunas (valor pode mudar com a atualização da base).",
        "local": False,
        "url": "https://bases.cinemateca.org.br/cgi-bin/wxis.exe/iah/?IsisScript=iah/iah.xis&base=FILMOGRAFIA&lang=p",
        "download_url": "https://github.com/anapaulagomes/cinemateca-brasileira/releases/download/v1/filmografia-15052025.zip",
    },
    "pescadores_e_pescadoras_profissionais": {
        "name": "pescadores_e_pescadoras_profissionais",
        "size": "large",
        "description": "Pescadores e pescadoras profissionais do Brasil, com dados de 2015 a 2024."
        "Contém dados como faixa de renda, nível de escolaridade, forma de atuação e localização."
        "Tem por volta de shape: 1.700.000 linhas e 10 colunas (valor pode mudar com a atualização da base).",
        "url": "https://dados.gov.br/dados/conjuntos-dados/base-de-dados-dos-registros-de-pescadores-e-pescadoras-profissionais",
        "local": True,
        "filepath": "pescadores-e-pescadoras-profissionais/pescadores-e-pescadoras-profissionais-07062025.parquet",
    },
    "salario_minimo": {
        "name": "salario_minimo_real_vigente",
        "size": "small",
        "description": "Salário mínimo real e vigente de 1940 a 2024."
        "Contém dados mensais do salário mínimo real (ajustado pela inflação) e o salário mínimo vigente (valor atual)."
        "Tem por volta de shape: 1.000 linhas e 3 colunas (valor pode mudar com a atualização da base).",
        "url": "http://www.ipeadata.gov.br/Default.aspx",
        "local": True,
        "filepath": "salario-minimo/salario-minimo-real-vigente-04062025.parquet",
    },
    "aldeias_indigenas": {
        "name": "aldeias_indigenas",
        "size": "small",
        "description": "Dados geoespaciais sobre aldeias indígenas, aldeias e coordenações regionais, técnicas locais e mapas das terras indígenas fornecidos pela Coordenação de Geoprocessamento da FUNAI. Tem por volta de 4.300 linhas e 13 colunas (valor pode mudar com a atualização da base).",
        "url": "https://dados.gov.br/dados/conjuntos-dados/tabela-de-aldeias-indgenas",
        "local": True,
        "filepath": "aldeias-indigenas/aldeias-indigenas-08062025.parquet",
    },
}


def _get_dataset_metadata(name: str) -> Dict:
    """
    Get metadata for a dataset, including datapackage information if available.

    Args:
        name: Name of the dataset

    Returns:
        Dictionary containing the dataset metadata
    """
    metadata = DATASETS_METADATA[name].copy()

    # if the dataset is not local and we have a datapackage.json, load its metadata
    if not metadata["local"]:
        datapackage_path = DATASETS_DIR / "datapackage.json"
        if datapackage_path.exists():
            datapackage = load_datapackage(datapackage_path)
            if datapackage.get("resources"):
                resource = datapackage["resources"][0]
                metadata.update(
                    {
                        "description": datapackage.get(
                            "description", metadata["description"]
                        ),
                        "size": f"{resource.get('bytes', 0) / 1024 / 1024:.1f}MB",
                        "filename": resource["path"],
                    }
                )

    return metadata


def list_datasets(include_metadata=False) -> Union[List[str], Dict[str, Dict]]:
    if include_metadata:
        return DATASETS_METADATA
    return list(DATASETS_METADATA.keys())
