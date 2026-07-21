-- Configuracao do Athena para consulta direta da camada Gold.
-- Roda uma vez para criar o banco e as 5 tabelas; MSCK REPAIR TABLE precisa
-- ser reexecutado sempre que uma nova particao (ano/uf) for gravada pelo
-- pipeline, para o Athena descobrir os novos diretorios.

CREATE DATABASE IF NOT EXISTS tech_challenge_gold;

-- ============================================================
-- indicador_vs_meta_municipal
-- Grao: id_municipio + ano. Particionada por ano e uf.
-- ============================================================
CREATE EXTERNAL
TABLE IF NOT EXISTS tech_challenge_gold.indicador_vs_meta_municipal (
    id_municipio string,
    rede string,
    taxa_alfabetizacao double,
    nivel_alfabetizacao double,
    percentual_participacao double,
    meta_alfabetizacao double,
    possui_resultado boolean,
    possui_meta boolean,
    baixa_confiabilidade boolean,
    deficit_meta double,
    nome_municipio string,
    media_portugues double,
    origem_ultimo_valor string,
    data_ultima_atualizacao string,
    data_ingestao_datalake string
) PARTITIONED BY (ano int, uf string) STORED AS PARQUET LOCATION 's3://tech-challenge-datalake-felipe/gold/indicador_vs_meta_municipal/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE tech_challenge_gold.indicador_vs_meta_municipal;

-- ============================================================
-- indicador_vs_meta_uf
-- Grao: sigla_uf + ano. Particionada so por ano (volume nao
-- justifica sub-particao por UF).
-- ============================================================
CREATE EXTERNAL
TABLE IF NOT EXISTS tech_challenge_gold.indicador_vs_meta_uf (
    sigla_uf string,
    rede string,
    taxa_alfabetizacao double,
    percentual_participacao double,
    meta_alfabetizacao double,
    possui_resultado boolean,
    possui_meta boolean,
    baixa_confiabilidade boolean,
    deficit_meta double,
    media_portugues double,
    origem_ultimo_valor string,
    data_ultima_atualizacao string,
    data_ingestao_datalake string
) PARTITIONED BY (ano int) STORED AS PARQUET LOCATION 's3://tech-challenge-datalake-felipe/gold/indicador_vs_meta_uf/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE tech_challenge_gold.indicador_vs_meta_uf;

-- ============================================================
-- indicador_vs_meta_brasil
-- Grao: ano. Arquivo unico, sem particionamento (volume de
-- 1 linha/ano nao justifica pasta por ano).
-- ============================================================
CREATE EXTERNAL
TABLE IF NOT EXISTS tech_challenge_gold.indicador_vs_meta_brasil (
    ano int,
    rede string,
    taxa_alfabetizacao double,
    percentual_participacao double,
    meta_alfabetizacao double,
    possui_resultado boolean,
    possui_meta boolean,
    baixa_confiabilidade boolean,
    deficit_meta double,
    origem_ultimo_valor string,
    data_ultima_atualizacao string,
    data_ingestao_datalake string
) STORED AS PARQUET LOCATION 's3://tech-challenge-datalake-felipe/gold/indicador_vs_meta_brasil/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

-- Sem particao, nao precisa de MSCK REPAIR.

-- ============================================================
-- desigualdade_por_rede_municipio
-- Grao: id_municipio + ano. Taxa e media de portugues
-- pivotadas por rede, gap municipal vs estadual ja calculado.
-- Rede Privada nao e reportada por sigilo estatistico da fonte,
-- por isso nao existe no schema.
-- Particionada por ano e uf.
-- ============================================================
CREATE EXTERNAL
TABLE IF NOT EXISTS tech_challenge_gold.desigualdade_por_rede_municipio (
    id_municipio string,
    nome_municipio string,
    taxa_alfabetizacao_municipal double,
    taxa_alfabetizacao_estadual double,
    taxa_alfabetizacao_publica double,
    gap_taxa_alfabetizacao_municipal_estadual double,
    media_portugues_municipal double,
    media_portugues_estadual double,
    media_portugues_publica double,
    gap_media_portugues_municipal_estadual double
) PARTITIONED BY (ano int, uf string) STORED AS PARQUET LOCATION 's3://tech-challenge-datalake-felipe/gold/desigualdade_por_rede_municipio/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK
REPAIR TABLE tech_challenge_gold.desigualdade_por_rede_municipio;

-- ============================================================
-- desigualdade_por_rede_uf
-- Mesmo desenho da tabela anterior, grao sigla_uf + ano.
-- Particionada so por ano.
-- ============================================================
CREATE EXTERNAL
TABLE IF NOT EXISTS tech_challenge_gold.desigualdade_por_rede_uf (
    sigla_uf string,
    taxa_alfabetizacao_municipal double,
    taxa_alfabetizacao_estadual double,
    taxa_alfabetizacao_publica double,
    gap_taxa_alfabetizacao_municipal_estadual double,
    media_portugues_municipal double,
    media_portugues_estadual double,
    media_portugues_publica double,
    gap_media_portugues_municipal_estadual double
) PARTITIONED BY (ano int) STORED AS PARQUET LOCATION 's3://tech-challenge-datalake-felipe/gold/desigualdade_por_rede_uf/' TBLPROPERTIES (
    'parquet.compression' = 'SNAPPY'
);

MSCK REPAIR TABLE tech_challenge_gold.desigualdade_por_rede_uf;