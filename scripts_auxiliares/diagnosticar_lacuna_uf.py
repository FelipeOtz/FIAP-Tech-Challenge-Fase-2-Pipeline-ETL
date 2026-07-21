"""
Script auxiliar para investigar por ano a lacuna de "sem resultado
correspondente" encontrada pelo qualidade_dados.py no nivel UF.

Nao faz parte do pipeline, e so uma ferramenta de diagnostico manual.

Rodar de dentro da pasta src/silver/:
    python diagnosticar_lacuna_uf.py
"""

import pandas as pd

from qualidade_dados import ler_todas_particoes
import boto3

s3_client = boto3.client("s3")

uf = ler_todas_particoes(s3_client, "uf")
meta_uf = ler_todas_particoes(s3_client, "meta_alfabetizacao_uf")

uf_filtrado = uf[uf["rede"] == "Pública (Estadual e Municipal)"].copy()
uf_filtrado["sigla_uf"] = uf_filtrado["sigla_uf"].astype(str)
meta_uf["sigla_uf"] = meta_uf["sigla_uf"].astype(str)

chaves_resultado = set(map(tuple, uf_filtrado[["sigla_uf", "ano"]].values))
chaves_meta = set(map(tuple, meta_uf[["sigla_uf", "ano"]].values))

sem_resultado = chaves_meta - chaves_resultado

anos_sem_resultado = [chave[1] for chave in sem_resultado]
print("Distribuicao por ano das combinacoes UF+ano sem resultado correspondente:")
print(pd.Series(anos_sem_resultado).value_counts().sort_index())

print("\nDetalhe das UFs sem resultado em anos que nao sejam 2025 (lacunas reais, nao esperadas):")
for sigla_uf, ano in sorted(sem_resultado, key=lambda item: item[1]):
    if ano != 2025:
        print(f"  {sigla_uf} - {ano}")
