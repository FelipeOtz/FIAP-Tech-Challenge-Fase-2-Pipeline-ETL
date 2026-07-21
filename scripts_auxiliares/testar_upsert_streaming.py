"""
Script auxiliar para testar isoladamente o upsert de streaming da Silver.

Nao faz parte do pipeline, e so uma ferramenta de teste manual. Liste os
eventos disponiveis em bronze/streaming/, aplica o primeiro na Silver via
transformacao_silver.lambda_handler, e confere o resultado gravado.

Rodar de dentro da pasta src/silver/:
    python testar_upsert_streaming.py
"""

import boto3
import pandas as pd

from transformacao_silver import BUCKET_DATALAKE, lambda_handler

s3_client = boto3.client("s3")


def listar_eventos_bronze_streaming() -> list:
    resposta = s3_client.list_objects_v2(Bucket=BUCKET_DATALAKE, Prefix="bronze/streaming/")
    return [obj["Key"] for obj in resposta.get("Contents", [])]


def main():
    chaves = listar_eventos_bronze_streaming()
    if not chaves:
        print("Nenhum evento encontrado em bronze/streaming/. Rode o producer e o consumer primeiro.")
        return

    chave_escolhida = chaves[0]
    print(f"Testando com o evento: {chave_escolhida}")

    resposta = s3_client.get_object(Bucket=BUCKET_DATALAKE, Key=chave_escolhida)
    evento_original = pd.read_json(resposta["Body"], typ="series")
    print("\nConteudo do evento:")
    print(evento_original)

    evento_fake = {"Records": [{"s3": {"object": {"key": chave_escolhida}}}]}
    resultado = lambda_handler(evento_fake, None)
    print("\nResultado do upsert:")
    print(resultado)

    id_municipio = str(evento_original["id_municipio"])
    ano = int(evento_original["ano"])

    for uf_tentativa in listar_ufs_gravadas(ano):
        chave_parquet = f"silver/municipio/ano={ano}/uf={uf_tentativa}/municipio.parquet"
        try:
            df = pd.read_parquet(f"s3://{BUCKET_DATALAKE}/{chave_parquet}")
        except FileNotFoundError:
            continue
        linha = df[df["id_municipio"].astype(str) == id_municipio]
        if not linha.empty:
            print(f"\nLinha encontrada em uf={uf_tentativa}:")
            print(linha[["id_municipio", "taxa_alfabetizacao", "origem_ultimo_valor", "data_ultima_atualizacao"]])
            return

    print("\nLinha nao encontrada em nenhuma particao de UF gravada. Confira o resultado do upsert acima.")


def listar_ufs_gravadas(ano: int) -> list:
    resposta = s3_client.list_objects_v2(
        Bucket=BUCKET_DATALAKE, Prefix=f"silver/municipio/ano={ano}/", Delimiter="/"
    )
    prefixos = [p["Prefix"] for p in resposta.get("CommonPrefixes", [])]
    return [p.split("uf=")[1].strip("/") for p in prefixos if "uf=" in p]


if __name__ == "__main__":
    main()
