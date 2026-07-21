-- Exemplos de consulta (DQL) sobre a camada Gold.
-- DDL (criacao de banco e tabelas) esta em athena_setup.sql.

-- ============================================================
-- Validacao unica cobrindo as 5 tabelas
-- Confirma que todas foram criadas e tem dado, sem precisar
-- rodar uma consulta por tabela.
-- ============================================================
SELECT 'indicador_vs_meta_municipal' AS tabela, COUNT(*) AS total_linhas
FROM tech_challenge_gold.indicador_vs_meta_municipal
UNION ALL
SELECT 'indicador_vs_meta_uf', COUNT(*)
FROM tech_challenge_gold.indicador_vs_meta_uf
UNION ALL
SELECT 'indicador_vs_meta_brasil', COUNT(*)
FROM tech_challenge_gold.indicador_vs_meta_brasil
UNION ALL
SELECT 'desigualdade_por_rede_municipio', COUNT(*)
FROM tech_challenge_gold.desigualdade_por_rede_municipio
UNION ALL
SELECT 'desigualdade_por_rede_uf', COUNT(*)
FROM tech_challenge_gold.desigualdade_por_rede_uf;

-- ============================================================
-- indicador_vs_meta_municipal
-- Politicas publicas: municipios que mais precisam de atencao,
-- ordenados pelo maior deficit em relacao a propria meta de 2024.
-- ============================================================
SELECT
    nome_municipio,
    uf,
    taxa_alfabetizacao,
    meta_alfabetizacao,
    deficit_meta,
    baixa_confiabilidade
FROM tech_challenge_gold.indicador_vs_meta_municipal
WHERE
    ano = 2024
    AND possui_meta = true
    AND possui_resultado = true
ORDER BY deficit_meta ASC
LIMIT 20;

-- ============================================================
-- indicador_vs_meta_uf
-- Politicas publicas: ranking de UFs pela distancia ate a propria
-- meta em 2024, incluindo os casos sem resultado medido (RR/DF).
-- ============================================================
SELECT
    sigla_uf,
    taxa_alfabetizacao,
    meta_alfabetizacao,
    deficit_meta,
    possui_resultado
FROM tech_challenge_gold.indicador_vs_meta_uf
WHERE
    ano = 2024
ORDER BY deficit_meta ASC;

-- ============================================================
-- indicador_vs_meta_brasil
-- Evolucao temporal do indicador nacional frente a meta, unico
-- arquivo, sem necessidade de particao no WHERE.
-- ============================================================
SELECT
    ano,
    taxa_alfabetizacao,
    meta_alfabetizacao,
    deficit_meta
FROM tech_challenge_gold.indicador_vs_meta_brasil
ORDER BY ano;

-- ============================================================
-- desigualdade_por_rede_municipio
-- Desigualdade educacional: municipios com maior gap entre rede
-- municipal e rede estadual em 2024. Rede Privada nao consta no
-- schema porque nao e reportada pela fonte nesse grao (0 linhas
-- em municipio.csv/uf.csv em qualquer ano, confirmado).
-- ============================================================
SELECT
    nome_municipio,
    uf,
    taxa_alfabetizacao_municipal,
    taxa_alfabetizacao_estadual,
    round(
        gap_taxa_alfabetizacao_municipal_estadual,
        2
    ) as gap_taxa_alfabetizacao_municipal_estadual
FROM tech_challenge_gold.desigualdade_por_rede_municipio
WHERE
    ano = 2024
    AND gap_taxa_alfabetizacao_municipal_estadual IS NOT NULL
ORDER BY
    gap_taxa_alfabetizacao_municipal_estadual DESC
LIMIT 20;

-- ============================================================
-- desigualdade_por_rede_uf
-- Mesmo gap agregado por UF, para visao de mapa/ranking estadual.
-- ============================================================
SELECT
    sigla_uf,
    taxa_alfabetizacao_municipal,
    taxa_alfabetizacao_estadual,
    round(
        gap_taxa_alfabetizacao_municipal_estadual,
        2
    ) as gap_taxa_alfabetizacao_municipal_estadual
FROM tech_challenge_gold.desigualdade_por_rede_uf
WHERE
    ano = 2024
ORDER BY
    gap_taxa_alfabetizacao_municipal_estadual DESC;