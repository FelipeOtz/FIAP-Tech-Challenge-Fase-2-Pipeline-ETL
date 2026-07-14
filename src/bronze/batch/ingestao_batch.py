"""
Ingestao batch da camada Bronze.

Le os arquivos da fonte externa (landing zone) e grava na camada Bronze do
data lake, particionando por ano quando a tabela possui essa coluna. Tabelas
de dimensao estatica (dicionario e os crosswalks de UF/municipio) sao
gravadas sem particao.

Formato de handler Lambda, com bloco de execucao local para testes antes do
deploy.
"""

import io
import logging
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_ORIGEM = "tech-challenge-fonte-externa-felipe"
BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_BRONZE = "bronze/batch"

# Whitelist fechada: nunca varrer o bucket de origem sem filtro.
ARQUIVOS_ESPERADOS = [
    "uf.csv",
    "municipio.csv",
    "meta_alfabetizacao_brasil.csv",
    "meta_alfabetizacao_uf.csv",
    "meta_alfabetizacao_municipio.csv",
    "dicionario.csv",
    "alunos.csv",
    "ibge_uf_map.csv",
    "ibge_municipios.csv",
]


def ler_csv_s3(s3_client, bucket: str, chave: str) -> pd.DataFrame:
    """Le um CSV do S3 diretamente para memoria, sem passar por disco local."""
    resposta = s3_client.get_object(Bucket=bucket, Key=chave)
    conteudo = resposta["Body"].read()
    return pd.read_csv(io.BytesIO(conteudo), low_memory=False)


def adicionar_metadado_ingestao(df: pd.DataFrame) -> pd.DataFrame:
    """Marca cada linha com o timestamp da execucao, para rastreabilidade."""
    df = df.copy()
    df["data_ingestao_datalake"] = datetime.now(timezone.utc).isoformat()
    return df


def gravar_csv_s3(s3_client, df: pd.DataFrame, bucket: str, chave: str) -> None:
    buffer = io.StringIO()
    df.to_csv(buffer, index=False)
    s3_client.put_object(Bucket=bucket, Key=chave, Body=buffer.getvalue())


def processar_arquivo(s3_client, nome_arquivo: str) -> dict:
    """
    Le um arquivo da fonte externa e grava na Bronze. Particiona por ano
    quando a coluna existe na tabela; caso contrario, grava como dimensao
    estatica, sem particao. Cada execucao grava um arquivo novo, marcado
    com o timestamp da ingestao, preservando o historico completo em vez
    de sobrescrever a versao anterior.
    """
    nome_tabela = nome_arquivo.replace(".csv", "")
    timestamp_execucao = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    df = ler_csv_s3(s3_client, BUCKET_ORIGEM, nome_arquivo)
    df = adicionar_metadado_ingestao(df)

    if "ano" in df.columns:
        anos_processados = []
        for ano, df_ano in df.groupby("ano"):
            nome_arquivo_destino = f"{nome_tabela}_{timestamp_execucao}.csv"
            chave_destino = f"{PREFIXO_BRONZE}/{nome_tabela}/ano={ano}/{nome_arquivo_destino}"
            gravar_csv_s3(s3_client, df_ano, BUCKET_DATALAKE, chave_destino)
            anos_processados.append(int(ano))
        logger.info("%s: %d linhas particionadas em %d anos", nome_tabela, len(df), len(anos_processados))
        return {"tabela": nome_tabela, "linhas": len(df), "anos": sorted(anos_processados)}

    nome_arquivo_destino = f"{nome_tabela}_{timestamp_execucao}.csv"
    chave_destino = f"{PREFIXO_BRONZE}/{nome_tabela}/{nome_arquivo_destino}"
    gravar_csv_s3(s3_client, df, BUCKET_DATALAKE, chave_destino)
    logger.info("%s: %d linhas gravadas sem particao", nome_tabela, len(df))
    return {"tabela": nome_tabela, "linhas": len(df), "anos": None}


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    resultados = []
    falhas = []

    for nome_arquivo in ARQUIVOS_ESPERADOS:
        try:
            resultados.append(processar_arquivo(s3_client, nome_arquivo))
        except Exception as erro:
            logger.error("Falha ao processar %s: %s", nome_arquivo, erro)
            falhas.append({"tabela": nome_arquivo, "erro": str(erro)})

    return {
        "status": "concluido_com_falhas" if falhas else "sucesso",
        "tabelas_processadas": len(resultados),
        "tabelas_com_falha": len(falhas),
        "detalhes": resultados,
        "falhas": falhas,
    }


if __name__ == "__main__":
    resultado = lambda_handler(None, None)
    print(resultado["status"])
    for item in resultado["detalhes"]:
        print(item)
    if resultado["falhas"]:
        print("Falhas:", resultado["falhas"])