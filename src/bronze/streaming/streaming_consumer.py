"""
Consumer de streaming da camada Bronze.

Le os eventos publicados pelo producer na area de staging, grava cada um
na Bronze particionado por ano, e remove o original do staging para
evitar reprocessamento. Pensado para rodar em intervalos curtos (via
EventBridge periodico na AWS, ou em loop local para teste).

Formato de handler Lambda, com bloco de execucao local para testes antes
do deploy.
"""

import json
import logging
import time
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

BUCKET_ORIGEM = "tech-challenge-fonte-externa-felipe"
BUCKET_DATALAKE = "tech-challenge-datalake-felipe"
PREFIXO_STAGING = "streaming/staging"
PREFIXO_BRONZE_STREAMING = "bronze/streaming"
INTERVALO_LOOP_LOCAL_SEGUNDOS = 60


def listar_eventos_staging(s3_client) -> list:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_ORIGEM, Prefix=f"{PREFIXO_STAGING}/")
    return [obj["Key"] for obj in resposta.get("Contents", [])]


def ler_evento(s3_client, chave: str) -> dict:
    resposta = s3_client.get_object(Bucket=BUCKET_ORIGEM, Key=chave)
    return json.loads(resposta["Body"].read())


def processar_evento(s3_client, chave_staging: str) -> dict:
    """Move um evento do staging (bucket de origem) para a Bronze (bucket do data lake), marcando o momento de consumo."""
    evento = ler_evento(s3_client, chave_staging)
    evento["data_ingestao_datalake"] = datetime.now(timezone.utc).isoformat()

    ano = evento["ano"]
    chave_destino = f"{PREFIXO_BRONZE_STREAMING}/ano={ano}/{evento['evento_id']}.json"
    s3_client.put_object(Bucket=BUCKET_DATALAKE, Key=chave_destino, Body=json.dumps(evento))
    s3_client.delete_object(Bucket=BUCKET_ORIGEM, Key=chave_staging)

    logger.info("Evento consumido: municipio=%s ano=%s destino=%s", evento["id_municipio"], ano, chave_destino)
    return {"evento_id": evento["evento_id"], "id_municipio": evento["id_municipio"], "ano": ano}


def lambda_handler(event, context):
    """
    Quando disparado por um gatilho S3 PUT, processa apenas o objeto que
    gerou o evento (evita corrida entre invocacoes concorrentes). Quando
    chamado localmente sem event (teste manual ou loop local), varre todo
    o staging pendente.
    """
    s3_client = boto3.client("s3")

    if event and "Records" in event:
        chaves_pendentes = [registro["s3"]["object"]["key"] for registro in event["Records"]]
    else:
        chaves_pendentes = listar_eventos_staging(s3_client)

    processados = []
    falhas = []

    for chave in chaves_pendentes:
        try:
            processados.append(processar_evento(s3_client, chave))
        except Exception as erro:
            logger.error("Falha ao processar %s: %s", chave, erro)
            falhas.append({"chave": chave, "erro": str(erro)})

    return {
        "status": "concluido_com_falhas" if falhas else "sucesso",
        "eventos_processados": len(processados),
        "eventos_com_falha": len(falhas),
        "detalhes": processados,
        "falhas": falhas,
    }


if __name__ == "__main__":
    print("Consumer rodando em loop local. Interrompa com Ctrl+C.")
    try:
        while True:
            resultado = lambda_handler(None, None)
            print(resultado)
            time.sleep(INTERVALO_LOOP_LOCAL_SEGUNDOS)
    except KeyboardInterrupt:
        print("Encerrado.")