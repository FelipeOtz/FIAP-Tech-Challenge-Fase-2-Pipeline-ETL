"""
Validacao de qualidade de dados da camada Silver.

Roda um conjunto fixo de checagens sobre as tabelas da Silver e retorna
um relatorio estruturado. Nao interrompe o pipeline em caso de achado --
os achados aqui documentados (ex: municipios sem meta correspondente) sao
lacunas de cobertura conhecidas da fonte, nao erros a serem corrigidos.
O relatorio serve como evidencia de governanca de dados.

Formato de handler Lambda, com bloco de execucao local para testes antes
do deploy.
"""

import io
import logging

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_SILVER = "silver"

REDE_MUNICIPAL_TEXTO = "Municipal"
REDE_PUBLICA_UF_TEXTO = "Pública (Estadual e Municipal)"


def ler_todas_particoes(s3_client, tabela: str) -> pd.DataFrame:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=f"{PREFIXO_SILVER}/{tabela}/")
    chaves = [obj["Key"] for obj in resposta.get("Contents", [])]
    if not chaves:
        raise FileNotFoundError(f"Nenhum arquivo encontrado para a tabela {tabela} na Silver")

    partes = []
    for chave in chaves:
        objeto = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave)
        partes.append(pd.read_parquet(io.BytesIO(objeto["Body"].read())))
    return pd.concat(partes, ignore_index=True)


def validar_duplicidade(df: pd.DataFrame, colunas_chave: list, nome_tabela: str) -> dict:
    duplicados = df.duplicated(subset=colunas_chave).sum()
    logger.info("%s: %d linhas duplicadas em %s", nome_tabela, duplicados, colunas_chave)
    return {"tabela": nome_tabela, "chave": colunas_chave, "linhas_duplicadas": int(duplicados)}


def validar_nulos(df: pd.DataFrame, colunas: list, nome_tabela: str) -> dict:
    nulos = {coluna: int(df[coluna].isnull().sum()) for coluna in colunas}
    logger.info("%s: nulos por coluna %s", nome_tabela, nulos)
    return {"tabela": nome_tabela, "nulos_por_coluna": nulos}


def validar_decodificacao(df: pd.DataFrame, colunas: list, nome_tabela: str) -> dict:
    """Verifica se algum codigo original nao encontrou correspondencia no dicionario apos o merge."""
    resultado = {}
    for coluna in colunas:
        coluna_codigo = f"{coluna}_codigo"
        if coluna_codigo not in df.columns:
            continue
        sem_traducao = df[df[coluna_codigo].notnull() & df[coluna].isnull()]
        resultado[coluna] = int(len(sem_traducao))
    logger.info("%s: codigos sem traducao no dicionario %s", nome_tabela, resultado)
    return {"tabela": nome_tabela, "sem_traducao_por_coluna": resultado}


def validar_integridade_referencial(
    df_resultado: pd.DataFrame, df_meta: pd.DataFrame, colunas_chave: list, nome_resultado: str, nome_meta: str
) -> dict:
    df_resultado = df_resultado.copy()
    df_meta = df_meta.copy()
    for coluna in colunas_chave:
        df_resultado[coluna] = df_resultado[coluna].astype(str)
        df_meta[coluna] = df_meta[coluna].astype(str)

    chaves_resultado = set(map(tuple, df_resultado[colunas_chave].values))
    chaves_meta = set(map(tuple, df_meta[colunas_chave].values))

    sem_meta = len(chaves_resultado - chaves_meta)
    sem_resultado = len(chaves_meta - chaves_resultado)

    logger.info(
        "%s vs %s: %d sem meta correspondente, %d sem resultado correspondente",
        nome_resultado, nome_meta, sem_meta, sem_resultado,
    )
    return {
        "resultado": nome_resultado,
        "meta": nome_meta,
        "sem_meta_correspondente": sem_meta,
        "sem_resultado_correspondente": sem_resultado,
    }


def lambda_handler(event, context):
    s3_client = boto3.client("s3")

    municipio = ler_todas_particoes(s3_client, "municipio")
    uf = ler_todas_particoes(s3_client, "uf")
    meta_municipio = ler_todas_particoes(s3_client, "meta_alfabetizacao_municipio")
    meta_uf = ler_todas_particoes(s3_client, "meta_alfabetizacao_uf")
    meta_brasil = ler_todas_particoes(s3_client, "meta_alfabetizacao_brasil")

    relatorio = {
        "duplicidade": [
            validar_duplicidade(municipio, ["id_municipio", "ano", "rede_codigo"], "municipio"),
            validar_duplicidade(uf, ["sigla_uf", "ano", "rede_codigo"], "uf"),
            validar_duplicidade(meta_municipio, ["id_municipio", "ano"], "meta_alfabetizacao_municipio"),
            validar_duplicidade(meta_uf, ["sigla_uf", "ano"], "meta_alfabetizacao_uf"),
            validar_duplicidade(meta_brasil, ["ano"], "meta_alfabetizacao_brasil"),
        ],
        "nulos": [
            validar_nulos(municipio, ["id_municipio", "ano", "rede", "taxa_alfabetizacao"], "municipio"),
            validar_nulos(uf, ["sigla_uf", "ano", "rede", "taxa_alfabetizacao"], "uf"),
            validar_nulos(meta_municipio, ["id_municipio", "ano", "taxa_alfabetizacao"], "meta_alfabetizacao_municipio"),
            validar_nulos(meta_uf, ["sigla_uf", "ano", "taxa_alfabetizacao"], "meta_alfabetizacao_uf"),
        ],
        "decodificacao": [
            validar_decodificacao(municipio, ["rede", "serie"], "municipio"),
            validar_decodificacao(uf, ["rede", "serie"], "uf"),
        ],
        "integridade_referencial": [
            validar_integridade_referencial(
                municipio[municipio["rede"] == REDE_MUNICIPAL_TEXTO],
                meta_municipio,
                ["id_municipio", "ano"],
                "municipio",
                "meta_alfabetizacao_municipio",
            ),
            validar_integridade_referencial(
                uf[uf["rede"] == REDE_PUBLICA_UF_TEXTO],
                meta_uf,
                ["sigla_uf", "ano"],
                "uf",
                "meta_alfabetizacao_uf",
            ),
        ],
    }

    return relatorio


if __name__ == "__main__":
    resultado = lambda_handler(None, None)
    for secao, achados in resultado.items():
        print(f"\n{secao.upper()}")
        for achado in achados:
            print(achado)