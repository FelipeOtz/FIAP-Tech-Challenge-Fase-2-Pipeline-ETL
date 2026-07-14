"""
Producer de streaming da camada Bronze.

Gera eventos simulando atualizacoes incrementais de desempenho por
municipio (por exemplo, uma nova medicao de taxa de alfabetizacao) e
publica cada evento como um arquivo JSON individual na area de staging
do data lake. O consumer, em execucao separada, le essa area e move os
eventos para a Bronze.

Formato de handler Lambda, com bloco de execucao local para testes antes
do deploy.
"""

import io
import json
import logging
import random
import uuid
from datetime import datetime, timezone

import boto3
import pandas as pd

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_ORIGEM = "tech-challenge-fonte-externa-felipe"
BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_STAGING = "streaming/staging"
ANO_REFERENCIA_PADRAO = 2024
REDE_MUNICIPAL = 3
QUANTIDADE_EVENTOS_PADRAO = 5


def carregar_amostra_municipios(s3_client, quantidade: int) -> list:
    """Seleciona codigos reais de municipio a partir do crosswalk oficial, para gerar eventos plausiveis."""
    resposta = s3_client.get_object(Bucket=BUCKET_ORIGEM, Key="ibge_municipios.csv")
    df = pd.read_csv(io.BytesIO(resposta["Body"].read()))
    codigos = df["codigo_municipio"].astype(str).tolist()
    return random.sample(codigos, min(quantidade, len(codigos)))


def gerar_evento(id_municipio: str, ano: int = ANO_REFERENCIA_PADRAO) -> dict:
    """Monta um evento simulando uma nova medicao de desempenho para o municipio informado."""
    return {
        "evento_id": str(uuid.uuid4()),
        "id_municipio": id_municipio,
        "ano": ano,
        "rede": REDE_MUNICIPAL,
        "taxa_alfabetizacao_atualizada": round(random.uniform(45.0, 92.0), 2),
        "tipo_evento": "ATUALIZACAO_DESEMPENHO",
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
    }


def publicar_evento(s3_client, evento: dict) -> str:
    chave = f"{PREFIXO_STAGING}/{evento['evento_id']}.json"
    s3_client.put_object(Bucket=BUCKET_ORIGEM, Key=chave, Body=json.dumps(evento))
    return chave


def lambda_handler(event, context):
    quantidade = (event or {}).get("quantidade_eventos", QUANTIDADE_EVENTOS_PADRAO)
    s3_client = boto3.client("s3")

    municipios_amostra = carregar_amostra_municipios(s3_client, quantidade)
    eventos_publicados = []

    for id_municipio in municipios_amostra:
        evento = gerar_evento(id_municipio)
        chave = publicar_evento(s3_client, evento)
        logger.info("Evento publicado: municipio=%s chave=%s", id_municipio, chave)
        eventos_publicados.append(chave)

    return {"status": "sucesso", "eventos_publicados": len(eventos_publicados), "chaves": eventos_publicados}


if __name__ == "__main__":
    resultado = lambda_handler({"quantidade_eventos": 5}, None)
    print(resultado)