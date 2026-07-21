"""
Construcao da camada Gold.

Le as tabelas da Silver e produz 5 tabelas finais, prontas para consumo
direto (Athena ou qualquer outra ferramenta), sem exigir transformacao
adicional de quem consulta:

- indicador_vs_meta_municipal, indicador_vs_meta_uf, indicador_vs_meta_brasil:
  resultado real comparado a meta, uma coluna de meta so (leitura diagonal
  pelo proprio ano da linha), enriquecidas com media_portugues quando a
  fonte bruta existir no grao.
- desigualdade_por_rede_municipio, desigualdade_por_rede_uf: taxa de
  alfabetizacao e media de portugues pivotadas por rede, com gap entre
  rede privada e rede publica ja calculado.

Cada evento de Silver aciona a reconstrucao apenas das tabelas Gold que
dependem daquela tabela de origem.

Formato de handler Lambda, com bloco de execucao local para testes antes
do deploy.
"""

import io
import logging
from urllib.parse import unquote_plus

import boto3
import pandas as pd
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_BRONZE_BATCH = "bronze/batch"
PREFIXO_SILVER = "silver"
PREFIXO_GOLD = "gold"

LIMIAR_PARTICIPACAO = 85
REDE_MUNICIPAL_TEXTO = "Municipal"
REDE_PUBLICA_UF_TEXTO = "Pública (Estadual e Municipal)"
MAPA_REDE_SUFIXO = {
    "Municipal": "municipal",
    "Estadual": "estadual",
    "Pública (Estadual e Municipal)": "publica",
}

DEPENDENCIAS_GOLD = {
    "meta_alfabetizacao_municipio": ["indicador_vs_meta_municipal"],
    "municipio": ["indicador_vs_meta_municipal", "desigualdade_por_rede_municipio"],
    "meta_alfabetizacao_uf": ["indicador_vs_meta_uf"],
    "uf": ["indicador_vs_meta_uf", "desigualdade_por_rede_uf"],
    "meta_alfabetizacao_brasil": ["indicador_vs_meta_brasil"],
}


def ler_particoes(s3_client, prefixo: str) -> pd.DataFrame:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=prefixo)
    chaves = [obj["Key"] for obj in resposta.get("Contents", [])]
    if not chaves:
        raise FileNotFoundError(f"Nenhum arquivo encontrado em {prefixo}")
    partes = [pd.read_parquet(io.BytesIO(s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=c)["Body"].read())) for c in chaves]
    return pd.concat(partes, ignore_index=True)


def ler_silver(s3_client, tabela: str, ano: int | None = None) -> pd.DataFrame:
    prefixo = f"{PREFIXO_SILVER}/{tabela}/" + (f"ano={ano}/" if ano is not None else "")
    return ler_particoes(s3_client, prefixo)


def ler_silver_opcional(s3_client, tabela: str, ano: int | None = None) -> pd.DataFrame | None:
    """Como ler_silver, mas retorna None em vez de lancar erro quando a particao nao existe -- usado para fontes de
    enriquecimento que podem legitimamente nao cobrir todos os anos (ex: uf.csv nao tem 2025)."""
    try:
        return ler_silver(s3_client, tabela, ano)
    except FileNotFoundError:
        return None


def ler_crosswalk_uf(s3_client) -> pd.DataFrame:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=f"{PREFIXO_BRONZE_BATCH}/ibge_uf_map/")
    chave = max(resposta["Contents"], key=lambda obj: obj["LastModified"])["Key"]
    conteudo = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave)["Body"].read()
    return pd.read_csv(io.BytesIO(conteudo))


def gravar_parquet_gold(s3_client, df: pd.DataFrame, chave: str) -> None:
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    s3_client.put_object(Bucket=BUCKET_DATALAKE, Key=chave, Body=buffer.getvalue())


def calcular_meta_diagonal(df: pd.DataFrame) -> pd.DataFrame:
    """Reduz as colunas meta_alfabetizacao_2024..2030 a uma unica coluna, lendo o valor do proprio ano da linha."""
    df = df.copy()
    colunas_meta = [c for c in df.columns if c.startswith("meta_alfabetizacao_20")]

    def valor_do_ano(linha):
        coluna = f"meta_alfabetizacao_{int(linha['ano'])}"
        return linha[coluna] if coluna in df.columns else None

    df["meta_alfabetizacao"] = df.apply(valor_do_ano, axis=1)
    return df.drop(columns=colunas_meta)


def calcular_campos_comuns(df: pd.DataFrame) -> pd.DataFrame:
    df = calcular_meta_diagonal(df)
    df["possui_resultado"] = df["taxa_alfabetizacao"].notnull()
    df["possui_meta"] = df["meta_alfabetizacao"].notnull()
    df["baixa_confiabilidade"] = df["percentual_participacao"] < LIMIAR_PARTICIPACAO
    df["deficit_meta"] = df["taxa_alfabetizacao"] - df["meta_alfabetizacao"]
    return df


def reconstruir_indicador_vs_meta_municipal(s3_client, ano: int) -> dict:
    df = calcular_campos_comuns(ler_silver(s3_client, "meta_alfabetizacao_municipio", ano))

    crosswalk_uf = ler_crosswalk_uf(s3_client)
    df["ibge_code"] = df["id_municipio"].astype(str).str[:2].astype(int)
    df = df.merge(crosswalk_uf, on="ibge_code", how="left").drop(columns=["ibge_code"])

    df_municipio = ler_silver_opcional(s3_client, "municipio", ano)
    if df_municipio is not None:
        enriquecimento = df_municipio[df_municipio["rede"] == REDE_MUNICIPAL_TEXTO]
        enriquecimento = enriquecimento[["id_municipio", "nome_municipio", "media_portugues"]].drop_duplicates(subset=["id_municipio"])
        df["id_municipio"] = df["id_municipio"].astype(str)
        enriquecimento["id_municipio"] = enriquecimento["id_municipio"].astype(str)
        df = df.merge(enriquecimento, on="id_municipio", how="left")
    else:
        df["nome_municipio"] = None
        df["media_portugues"] = None
        logger.info("municipio ano=%s sem particao de resultado bruto na silver; enriquecimento pulado", ano)

    particoes = []
    for uf, grupo in df.groupby("uf"):
        chave = f"{PREFIXO_GOLD}/indicador_vs_meta_municipal/ano={ano}/uf={uf}/indicador_vs_meta_municipal.parquet"
        gravar_parquet_gold(s3_client, grupo, chave)
        particoes.append(chave)

    logger.info("indicador_vs_meta_municipal ano=%s: %d particoes de UF gravadas", ano, len(particoes))
    return {"tabela": "indicador_vs_meta_municipal", "ano": ano, "linhas": len(df), "particoes": len(particoes)}


def reconstruir_indicador_vs_meta_uf(s3_client, ano: int) -> dict:
    df = calcular_campos_comuns(ler_silver(s3_client, "meta_alfabetizacao_uf", ano))

    df_uf = ler_silver_opcional(s3_client, "uf", ano)
    if df_uf is not None:
        enriquecimento = df_uf[df_uf["rede"] == REDE_PUBLICA_UF_TEXTO]
        enriquecimento = enriquecimento[["sigla_uf", "media_portugues"]].drop_duplicates(subset=["sigla_uf"])
        df = df.merge(enriquecimento, on="sigla_uf", how="left")
    else:
        df["media_portugues"] = None
        logger.info("uf ano=%s sem particao de resultado bruto na silver; media_portugues fica nulo nesse ano", ano)

    chave = f"{PREFIXO_GOLD}/indicador_vs_meta_uf/ano={ano}/indicador_vs_meta_uf.parquet"
    gravar_parquet_gold(s3_client, df, chave)
    logger.info("indicador_vs_meta_uf ano=%s: %d linhas gravadas", ano, len(df))
    return {"tabela": "indicador_vs_meta_uf", "ano": ano, "linhas": len(df), "particoes": 1}


def reconstruir_indicador_vs_meta_brasil(s3_client) -> dict:
    df = calcular_campos_comuns(ler_silver(s3_client, "meta_alfabetizacao_brasil"))
    chave = f"{PREFIXO_GOLD}/indicador_vs_meta_brasil/indicador_vs_meta_brasil.parquet"
    gravar_parquet_gold(s3_client, df, chave)
    logger.info("indicador_vs_meta_brasil: %d linhas gravadas", len(df))
    return {"tabela": "indicador_vs_meta_brasil", "ano": None, "linhas": len(df), "particoes": 1}


def pivotar_por_rede(df: pd.DataFrame, colunas_grao: list) -> pd.DataFrame:
    df_filtrado = df[df["rede"].isin(MAPA_REDE_SUFIXO)].copy()
    df_filtrado["rede_sufixo"] = df_filtrado["rede"].map(MAPA_REDE_SUFIXO)

    pivot = df_filtrado.pivot_table(
        index=colunas_grao, columns="rede_sufixo", values=["taxa_alfabetizacao", "media_portugues"]
    )
    pivot.columns = [f"{valor}_{sufixo}" for valor, sufixo in pivot.columns]
    pivot = pivot.reset_index()

    if "taxa_alfabetizacao_municipal" in pivot.columns and "taxa_alfabetizacao_estadual" in pivot.columns:
        pivot["gap_taxa_alfabetizacao_municipal_estadual"] = pivot["taxa_alfabetizacao_municipal"] - pivot["taxa_alfabetizacao_estadual"]
    if "media_portugues_municipal" in pivot.columns and "media_portugues_estadual" in pivot.columns:
        pivot["gap_media_portugues_municipal_estadual"] = pivot["media_portugues_municipal"] - pivot["media_portugues_estadual"]

    return pivot


def reconstruir_desigualdade_por_rede_municipio(s3_client, ano: int) -> dict:
    df_municipio = ler_silver(s3_client, "municipio", ano)
    pivot = pivotar_por_rede(df_municipio, colunas_grao=["id_municipio", "ano"])

    identificacao = df_municipio[["id_municipio", "nome_municipio", "uf"]].drop_duplicates(subset=["id_municipio"])
    pivot = pivot.merge(identificacao, on="id_municipio", how="left")

    particoes = []
    for uf, grupo in pivot.groupby("uf"):
        chave = f"{PREFIXO_GOLD}/desigualdade_por_rede_municipio/ano={ano}/uf={uf}/desigualdade_por_rede_municipio.parquet"
        gravar_parquet_gold(s3_client, grupo, chave)
        particoes.append(chave)

    logger.info("desigualdade_por_rede_municipio ano=%s: %d particoes de UF gravadas", ano, len(particoes))
    return {"tabela": "desigualdade_por_rede_municipio", "ano": ano, "linhas": len(pivot), "particoes": len(particoes)}


def reconstruir_desigualdade_por_rede_uf(s3_client, ano: int) -> dict:
    df_uf = ler_silver(s3_client, "uf", ano)
    pivot = pivotar_por_rede(df_uf, colunas_grao=["sigla_uf", "ano"])

    chave = f"{PREFIXO_GOLD}/desigualdade_por_rede_uf/ano={ano}/desigualdade_por_rede_uf.parquet"
    gravar_parquet_gold(s3_client, pivot, chave)
    logger.info("desigualdade_por_rede_uf ano=%s: %d linhas gravadas", ano, len(pivot))
    return {"tabela": "desigualdade_por_rede_uf", "ano": ano, "linhas": len(pivot), "particoes": 1}


CONSTRUTORES = {
    "indicador_vs_meta_municipal": reconstruir_indicador_vs_meta_municipal,
    "indicador_vs_meta_uf": reconstruir_indicador_vs_meta_uf,
    "desigualdade_por_rede_municipio": reconstruir_desigualdade_por_rede_municipio,
    "desigualdade_por_rede_uf": reconstruir_desigualdade_por_rede_uf,
}


def parse_chave_silver(chave: str) -> tuple:
    partes = chave.split("/")
    tabela = partes[1]
    ano = next((int(p.split("=")[1]) for p in partes if p.startswith("ano=")), None)
    return tabela, ano


def processar_evento(s3_client, chave: str) -> list:
    tabela, ano = parse_chave_silver(chave)
    tabelas_gold_afetadas = DEPENDENCIAS_GOLD.get(tabela, [])

    if not tabelas_gold_afetadas:
        logger.info("Chave %s (tabela=%s) nao afeta nenhuma tabela gold", chave, tabela)
        return [{"chave": chave, "acao": "ignorado"}]

    if tabela == "meta_alfabetizacao_brasil":
        return [reconstruir_indicador_vs_meta_brasil(s3_client)]

    return [CONSTRUTORES[tabela_gold](s3_client, ano) for tabela_gold in tabelas_gold_afetadas]


def listar_anos_disponiveis(s3_client, tabela: str) -> list:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix=f"{PREFIXO_SILVER}/{tabela}/", Delimiter="/")
    prefixos = [p["Prefix"] for p in resposta.get("CommonPrefixes", [])]
    return sorted(int(p.split("ano=")[1].strip("/")) for p in prefixos if "ano=" in p)


def reconstruir_tudo(s3_client) -> list:
    resultados = []
    for ano in listar_anos_disponiveis(s3_client, "meta_alfabetizacao_municipio"):
        resultados.append(reconstruir_indicador_vs_meta_municipal(s3_client, ano))
    for ano in listar_anos_disponiveis(s3_client, "municipio"):
        resultados.append(reconstruir_desigualdade_por_rede_municipio(s3_client, ano))
    for ano in listar_anos_disponiveis(s3_client, "meta_alfabetizacao_uf"):
        resultados.append(reconstruir_indicador_vs_meta_uf(s3_client, ano))
    for ano in listar_anos_disponiveis(s3_client, "uf"):
        resultados.append(reconstruir_desigualdade_por_rede_uf(s3_client, ano))
    resultados.append(reconstruir_indicador_vs_meta_brasil(s3_client))
    return resultados


def lambda_handler(event, context):
    s3_client = boto3.client("s3")
    resultados = []
    falhas = []

    if event and "Records" in event:
        for registro in event["Records"]:
            chave = unquote_plus(registro["s3"]["object"]["key"])
            try:
                resultados.extend(processar_evento(s3_client, chave))
            except Exception as erro:
                logger.error("Falha ao processar %s: %s", chave, erro)
                falhas.append({"chave": chave, "erro": str(erro)})
    else:
        resultados = reconstruir_tudo(s3_client)

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