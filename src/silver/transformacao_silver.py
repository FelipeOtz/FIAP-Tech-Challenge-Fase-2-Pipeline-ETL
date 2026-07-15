"""
Transformacao da camada Silver.

Dois modos de operacao, decididos pela origem do evento que aciona o
handler:

- Chave em bronze/batch/: reconstroi a particao (tabela, ano) inteira na
  Silver a partir do arquivo mais recente da Bronze, aplicando
  decodificacao do dicionario, enriquecimento com os crosswalks de UF e
  nome de municipio, e limpeza basica.
- Chave em bronze/streaming/: aplica um upsert pontual na linha do
  municipio correspondente, sem reconstruir a particao inteira.

Formato de handler Lambda, com bloco de execucao local para testes antes
do deploy.
"""

import io
import json
import logging
from datetime import datetime, timezone
from urllib.parse import unquote_plus

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_BRONZE_BATCH = "bronze/batch"
PREFIXO_BRONZE_STREAMING = "bronze/streaming"
PREFIXO_SILVER = "silver"

TABELAS_SILVER = {"municipio", "uf", "meta_alfabetizacao_municipio", "meta_alfabetizacao_uf", "meta_alfabetizacao_brasil"}
REDE_MUNICIPAL = 3


def obter_chave_mais_recente(s3_client, prefixo: str) -> str:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=prefixo)
    objetos = resposta.get("Contents", [])
    if not objetos:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {prefixo}")
    mais_recente = max(objetos, key=lambda obj: obj["LastModified"])
    return mais_recente["Key"]


def ler_csv_bronze(s3_client, prefixo: str) -> pd.DataFrame:
    chave = obter_chave_mais_recente(s3_client, prefixo)
    resposta = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave)
    return pd.read_csv(io.BytesIO(resposta["Body"].read()), low_memory=False)


def ler_parquet_silver(s3_client, chave: str) -> pd.DataFrame | None:
    try:
        resposta = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave)
        return pd.read_parquet(io.BytesIO(resposta["Body"].read()))
    except ClientError as erro:
        if erro.response["Error"]["Code"] == "NoSuchKey":
            return None
        raise


def gravar_parquet_silver(s3_client, df: pd.DataFrame, chave: str) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_DATALAKE, Key=chave, Body=buffer.getvalue())


def decodificar_coluna(df: pd.DataFrame, dicionario: pd.DataFrame, nome_tabela: str, coluna: str) -> pd.DataFrame:
    """Substitui o codigo pelo texto correspondente, preservando o codigo original em {coluna}_codigo."""
    mapa = dicionario[(dicionario["id_tabela"] == nome_tabela) & (dicionario["nome_coluna"] == coluna)]
    mapa = dict(zip(mapa["chave"].astype(str), mapa["valor"]))
    df = df.copy()
    df[f"{coluna}_codigo"] = df[coluna]
    df[coluna] = df[coluna].astype(str).map(mapa)
    return df


def derivar_uf(df: pd.DataFrame, crosswalk_uf: pd.DataFrame, coluna_id_municipio: str = "id_municipio") -> pd.DataFrame:
    df = df.copy()
    df["ibge_code"] = df[coluna_id_municipio].astype(str).str[:2].astype(int)
    df = df.merge(crosswalk_uf, on="ibge_code", how="left")
    return df.drop(columns=["ibge_code"])


def anexar_nome_municipio(df: pd.DataFrame, crosswalk_nomes: pd.DataFrame) -> pd.DataFrame:
    crosswalk_nomes = crosswalk_nomes.rename(columns={"codigo_municipio": "id_municipio"})
    crosswalk_nomes["id_municipio"] = crosswalk_nomes["id_municipio"].astype(str)
    df = df.copy()
    df["id_municipio"] = df["id_municipio"].astype(str)
    return df.merge(crosswalk_nomes, on="id_municipio", how="left")


def carregar_lookups(s3_client) -> dict:
    return {
        "dicionario": ler_csv_bronze(s3_client, f"{PREFIXO_BRONZE_BATCH}/dicionario/"),
        "crosswalk_uf": ler_csv_bronze(s3_client, f"{PREFIXO_BRONZE_BATCH}/ibge_uf_map/"),
        "crosswalk_nomes": ler_csv_bronze(s3_client, f"{PREFIXO_BRONZE_BATCH}/ibge_municipios/"),
    }


def reconstruir_particao_batch(s3_client, tabela: str, ano: int, lookups: dict) -> dict:
    """Le o arquivo mais recente da particao (tabela, ano) na Bronze e regrava a versao tratada na Silver."""
    prefixo_origem = f"{PREFIXO_BRONZE_BATCH}/{tabela}/ano={ano}/"
    df = ler_csv_bronze(s3_client, prefixo_origem)
    df["origem_ultimo_valor"] = "batch"
    df["data_ultima_atualizacao"] = datetime.now(timezone.utc).isoformat()

    if "id_municipio" in df.columns:
        df["id_municipio"] = df["id_municipio"].astype(str)

    if tabela == "municipio":
        df = decodificar_coluna(df, lookups["dicionario"], "municipio", "rede")
        df = decodificar_coluna(df, lookups["dicionario"], "municipio", "serie")
        df = derivar_uf(df, lookups["crosswalk_uf"])
        df = anexar_nome_municipio(df, lookups["crosswalk_nomes"])
        particoes_gravadas = []
        for uf, grupo in df.groupby("uf"):
            chave = f"{PREFIXO_SILVER}/municipio/ano={ano}/uf={uf}/municipio.parquet"
            gravar_parquet_silver(s3_client, grupo, chave)
            particoes_gravadas.append(chave)
        logger.info("municipio ano=%s: %d particoes de UF gravadas", ano, len(particoes_gravadas))
        return {"tabela": tabela, "ano": ano, "linhas": len(df), "particoes": len(particoes_gravadas)}

    if tabela == "uf":
        df = decodificar_coluna(df, lookups["dicionario"], "uf", "rede")
        df = decodificar_coluna(df, lookups["dicionario"], "uf", "serie")

    if tabela == "meta_alfabetizacao_brasil":
        chave_destino = f"{PREFIXO_SILVER}/{tabela}/{tabela}.parquet"
        df_existente = ler_parquet_silver(s3_client, chave_destino)
        if df_existente is not None:
            df_existente = df_existente[df_existente["ano"] != ano]
            df = pd.concat([df_existente, df], ignore_index=True).sort_values("ano")
        gravar_parquet_silver(s3_client, df, chave_destino)
        logger.info("%s: %d linhas totais (ano=%s mesclado, sem particao)", tabela, len(df), ano)
        return {"tabela": tabela, "ano": ano, "linhas": len(df), "particoes": 1}

    chave_destino = f"{PREFIXO_SILVER}/{tabela}/ano={ano}/{tabela}.parquet"
    gravar_parquet_silver(s3_client, df, chave_destino)
    logger.info("%s ano=%s: %d linhas gravadas em %s", tabela, ano, len(df), chave_destino)
    return {"tabela": tabela, "ano": ano, "linhas": len(df), "particoes": 1}


def aplicar_evento_streaming(s3_client, chave_evento: str, lookups: dict) -> dict:
    """Atualiza pontualmente a linha do municipio na Silver, sem reconstruir a particao inteira."""
    resposta = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave_evento)
    evento = json.loads(resposta["Body"].read())

    id_municipio = str(evento["id_municipio"])
    ano = evento["ano"]
    ibge_code = int(id_municipio[:2])
    uf = lookups["crosswalk_uf"].set_index("ibge_code").loc[ibge_code, "uf"]
    rede_texto = decodificar_coluna(
        pd.DataFrame([{"rede": evento["rede"]}]), lookups["dicionario"], "municipio", "rede"
    )["rede"].iloc[0]

    chave_silver = f"{PREFIXO_SILVER}/municipio/ano={ano}/uf={uf}/municipio.parquet"
    df = ler_parquet_silver(s3_client, chave_silver)

    linha_existe = df is not None and (
        (df["id_municipio"].astype(str) == id_municipio) & (df["rede"] == rede_texto)
    ).any()

    if linha_existe:
        indice = df.index[(df["id_municipio"].astype(str) == id_municipio) & (df["rede"] == rede_texto)]
        df.loc[indice, "taxa_alfabetizacao"] = evento["taxa_alfabetizacao_atualizada"]
        df.loc[indice, "origem_ultimo_valor"] = "streaming"
        df.loc[indice, "data_ultima_atualizacao"] = evento["timestamp_evento"]
        acao = "atualizado"
    else:
        nome_municipio = lookups["crosswalk_nomes"].set_index("codigo_municipio")["nome_municipio"].get(int(id_municipio))
        linha_nova = pd.DataFrame([{
            "id_municipio": id_municipio,
            "ano": ano,
            "uf": uf,
            "nome_municipio": nome_municipio,
            "rede": rede_texto,
            "taxa_alfabetizacao": evento["taxa_alfabetizacao_atualizada"],
            "origem_ultimo_valor": "streaming",
            "data_ultima_atualizacao": evento["timestamp_evento"],
        }])
        df = linha_nova if df is None else pd.concat([df, linha_nova], ignore_index=True)
        acao = "criado"

    gravar_parquet_silver(s3_client, df, chave_silver)
    logger.info("Evento de streaming %s: municipio=%s ano=%s uf=%s", acao, id_municipio, ano, uf)
    return {"evento_id": evento["evento_id"], "id_municipio": id_municipio, "acao": acao}


def parse_chave_bronze_batch(chave: str) -> tuple:
    partes = chave.split("/")
    tabela = partes[2]
    ano = next((int(p.split("=")[1]) for p in partes if p.startswith("ano=")), None)
    return tabela, ano


def processar_evento(s3_client, chave: str, lookups: dict) -> dict:
    if chave.startswith(PREFIXO_BRONZE_BATCH):
        tabela, ano = parse_chave_bronze_batch(chave)
        if tabela not in TABELAS_SILVER or ano is None:
            logger.info("Chave %s (tabela=%s) fora do escopo de output da silver, ignorada", chave, tabela)
            return {"chave": chave, "acao": "ignorado"}
        return reconstruir_particao_batch(s3_client, tabela, ano, lookups)

    if chave.startswith(PREFIXO_BRONZE_STREAMING):
        return aplicar_evento_streaming(s3_client, chave, lookups)

    raise ValueError(f"Chave fora do escopo esperado: {chave}")


def listar_anos_disponiveis(s3_client, tabela: str) -> list:
    resposta = s3_client.list_objects_v2(
        Bucket=BUCKET_DATALAKE, Prefix=f"{PREFIXO_BRONZE_BATCH}/{tabela}/", Delimiter="/"
    )
    prefixos = [p["Prefix"] for p in resposta.get("CommonPrefixes", [])]
    return sorted(int(p.split("ano=")[1].strip("/")) for p in prefixos if "ano=" in p)


def reconstruir_tudo(s3_client, lookups: dict) -> list:
    """Reconstroi todas as particoes de todas as tabelas com output na Silver, a partir do batch mais recente."""
    resultados = []
    for tabela in TABELAS_SILVER:
        for ano in listar_anos_disponiveis(s3_client, tabela):
            resultados.append(reconstruir_particao_batch(s3_client, tabela, ano, lookups))
    return resultados


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    lookups = carregar_lookups(s3_client)

    resultados = []
    falhas = []

    if event and "Records" in event:
        for registro in event["Records"]:
            chave = unquote_plus(registro["s3"]["object"]["key"])
            try:
                resultados.append(processar_evento(s3_client, chave, lookups))
            except Exception as erro:
                logger.error("Falha ao processar %s: %s", chave, erro)
                falhas.append({"chave": chave, "erro": str(erro)})
    else:
        resultados = reconstruir_tudo(s3_client, lookups)

    return {
        "status": "concluido_com_falhas" if falhas else "sucesso",
        "itens_processados": len(resultados),
        "itens_com_falha": len(falhas),
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