# Tech Challenge Fase 2

## Pipeline Híbrido para Análise da Alfabetização no Brasil

Projeto integrador da Fase 2 da pós-graduação em Engenharia de Dados, simulando o trabalho de um time de engenharia de dados de uma organização pública de análise educacional.

---

## Sumário

- [Contexto do Problema](#contexto-do-problema)
- [Fontes de Dados](#fontes-de-dados)
- [Arquitetura da Solução](#arquitetura-da-solução)
- [Achados da Exploração de Dados (EDA)](#achados-da-exploração-de-dados-eda)
- [Decisões Arquiteturais e Trade-offs](#decisões-arquiteturais-e-trade-offs)
- [Estrutura do Repositório](#estrutura-do-repositório)
- [Camadas do Pipeline](#camadas-do-pipeline)
- [Qualidade de Dados](#qualidade-de-dados)
- [FinOps: Otimização de Custos](#finops--otimização-de-custos)
- [Monitoramento](#monitoramento)
- [Aplicação em Inteligência Artificial](#aplicação-em-inteligência-artificial)
- [Como Executar Localmente](#como-executar-localmente)
- [Deploy na AWS](#deploy-na-aws)
- [Restrições do Ambiente](#restrições-do-ambiente)
- [Lições Aprendidas](#lições-aprendidas)
- [Nota Pessoal sobre o Prazo de Entrega](#nota-pessoal-sobre-o-prazo-de-entrega)

---

## Contexto do Problema

A alfabetização na infância é um dos pilares fundamentais para o desenvolvimento educacional, social e econômico de um país. O **Compromisso Nacional Criança Alfabetizada** mobiliza União, estados, Distrito Federal e municípios para garantir que todas as crianças brasileiras estejam alfabetizadas até o final do 2º ano do ensino fundamental.

Em 2023, o INEP (Instituto Nacional de Estudos e Pesquisas Educacionais Anísio Teixeira) realizou a Pesquisa Alfabetiza Brasil, que definiu o ponto de corte de 743 pontos na escala de proficiência do Saeb: a partir do qual uma criança é considerada alfabetizada. Com base nesse parâmetro, foi criado o **Indicador Criança Alfabetizada**, e a meta nacional é que 100% das crianças estejam alfabetizadas até 2030.

Entender os fatores que influenciam esse processo exige integrar fontes heterogêneas (metas nacionais, estaduais e municipais, dados territoriais e indicadores de desempenho), cada uma com granularidade, formato e cobertura temporal próprias. Este projeto constrói um **pipeline de dados híbrido (batch + streaming)** na **AWS**, seguindo a **Arquitetura Medalhão** (Bronze/Silver/Gold), para integrar essas fontes de forma confiável, particionada e pronta para análise, sem exigir transformação adicional de quem consome a camada final. A escolha de nuvem e as demais decisões de arquitetura estão detalhadas na seção [Decisões Arquiteturais e Trade-offs](#decisões-arquiteturais-e-trade-offs).

---

## Fontes de Dados

Todos os arquivos vêm da plataforma [Base dos Dados](https://basedosdados.org/), com exceção do crosswalk de municípios (IBGE). A Base dos Dados disponibiliza esses dados para consulta e download (via console web ou BigQuery), mas não oferece uma API de extração automatizável dentro do escopo e do tempo deste projeto: não há como um job de produção se conectar a ela periodicamente e puxar os dados sozinho, como faria contra um banco de dados ou uma API REST tradicional.

Diante dessa restrição real da fonte, a solução adotada foi baixar os 9 arquivos manualmente e enviá-los (upload manual) para o bucket `tech-challenge-fonte-externa-felipe`, que atua como **landing zone**: o ponto de entrada onde, em produção, um job agendado depositaria os dados extraídos automaticamente. Esse upload manual é a única etapa não automatizada de toda a ingestão batch; a partir do momento em que o arquivo está na landing zone, tudo o que acontece depois (leitura, particionamento, gravação na camada Bronze) é real, automatizado e roda via Lambda, sem intervenção manual.

| Arquivo                            | Granularidade                  | Descrição                                                                                                                                                                                                                                                                                               |
| ---------------------------------- | ------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `municipio.csv`                    | Município × Ano × Série × Rede | Resultado real medido (taxa de alfabetização, média de português, distribuição de proficiência)                                                                                                                                                                                                         |
| `uf.csv`                           | UF × Ano × Série × Rede        | Mesmos indicadores agregados por estado                                                                                                                                                                                                                                                                 |
| `meta_alfabetizacao_municipio.csv` | Município × Ano                | Meta pactuada 2024–2030 + resultado real da rede Municipal, já na mesma linha                                                                                                                                                                                                                           |
| `meta_alfabetizacao_uf.csv`        | UF × Ano                       | Meta pactuada 2024–2030 + resultado real da rede Pública, já na mesma linha                                                                                                                                                                                                                             |
| `meta_alfabetizacao_brasil.csv`    | Brasil × Ano                   | Meta pactuada 2024–2030 + resultado real nacional, já na mesma linha                                                                                                                                                                                                                                    |
| `dicionario.csv`                   | -                              | Tabela de lookup genérica (`id_tabela` + `nome_coluna` + `chave` → `valor`), decodifica `rede` e `serie` em múltiplas tabelas                                                                                                                                                                           |
| `alunos.csv`                       | Aluno (microdados)             | ~3,87 milhões de registros; mantida bruta na Bronze, fora do escopo de Silver/Gold                                                                                                                                                                                                                      |
| `ibge_uf_map.csv`                  | UF                             | Crosswalk `ibge_code` → `uf`. Fonte externa: [IBGE, Códigos dos Municípios](https://www.ibge.gov.br/explica/codigos-dos-municipios.php), que documenta a numeração oficial de UF usada nos 2 primeiros dígitos do código de 7 dígitos do município; montado pelo time a partir dessa numeração (27 UFs) |
| `ibge_municipios.csv`              | Município                      | Crosswalk `codigo_municipio` → `nome_municipio`, 5.571 municípios. Fonte externa, baixada diretamente de: [IBGE, Códigos dos Municípios](https://www.ibge.gov.br/explica/codigos-dos-municipios.php)                                                                                                    |

**Por que 9 arquivos, não 7:** as tabelas de meta em nível município e UF não possuem coluna de UF nem de nome de município: só o código IBGE. Os dois crosswalks foram adicionados à fonte externa especificamente para resolver isso sem embutir tabelas de mapeamento fixas no código.

---

## Arquitetura da Solução

```
┌──────────────────────────────────────────────────────────┐
│         FONTE EXTERNA / LANDING ZONE (S3)                │
│         tech-challenge-fonte-externa-felipe               │
│  9 arquivos CSV (7 tabelas de negócio + 2 crosswalks)     │
│  streaming/staging/  ← eventos do producer aguardando     │
└───────────────┬────────────────────────┬──────────────────┘
                │                        │
                ▼                        ▼
      ┌──────────────────┐   ┌────────────────────────┐
      │  INGESTÃO BATCH   │   │  INGESTÃO STREAMING     │
      │  Lambda handler    │   │  Producer (EventBridge  │
      │  (agendada ou      │   │  a cada 5 min) → S3      │
      │  manual)            │   │  staging → S3 PUT        │
      │                     │   │  aciona Consumer         │
      └─────────┬───────────┘   └───────────┬───────────────┘
                │                            │
                ▼                            ▼
┌────────────────────────────────────────────────────────────┐
│              DATA LAKE (S3): tech-challenge-datalake-felipe │
│                                                                │
│  BRONZE (dados brutos, CSV/JSON, sem transformação)           │
│    bronze/batch/{tabela}/ano={ano}/{tabela}_{timestamp}.csv   │
│    bronze/batch/{tabela}/{tabela}_{timestamp}.csv  (sem ano)  │
│    bronze/streaming/ano={ano}/{evento_id}.json                │
│         │ aciona (S3 PUT: bronze/batch/ e bronze/streaming/)  │
│         ▼                                                     │
│  SILVER (limpo, decodificado, enriquecido; Parquet)           │
│    silver/municipio/ano={ano}/uf={uf}/                        │
│    silver/uf/ano={ano}/                                       │
│    silver/meta_alfabetizacao_municipio/ano={ano}/              │
│    silver/meta_alfabetizacao_uf/ano={ano}/                    │
│    silver/meta_alfabetizacao_brasil/  (arquivo único, sem ano) │
│         │ aciona (S3 PUT: silver/)                             │
│         ▼                                                     │
│  GOLD (pronto para consumo, sem transformação adicional)       │
│    gold/indicador_vs_meta_municipal/ano={ano}/uf={uf}/         │
│    gold/indicador_vs_meta_uf/ano={ano}/                        │
│    gold/indicador_vs_meta_brasil/  (arquivo único)              │
│    gold/desigualdade_por_rede_municipio/ano={ano}/uf={uf}/      │
│    gold/desigualdade_por_rede_uf/ano={ano}/                     │
└───────────────────────────┬────────────────────────────────────┘
                            ▼
                   ┌──────────────────┐
                   │   AWS Athena      │
                   │  (schema-on-read) │
                   └──────────────────┘
```

**Fluxo de dados, resumido:** cada gravação de partição na Bronze dispara automaticamente a Lambda de transformação da Silver (evento S3 PUT), que por sua vez dispara a Lambda de construção da Gold. É uma arquitetura orientada a evento de ponta a ponta: nenhuma camada precisa ser executada manualmente depois que um dado novo chega, seja via batch ou via streaming.

---

## Achados da Exploração de Dados (EDA)

Num pipeline que integra fontes heterogêneas, a maior fonte de risco não é a transformação em si, é assumir uma estrutura de dado que parece óbvia mas não é: um dicionário que parece decodificar uma coluna só e na verdade cobre várias, uma categoria que existe no schema mas nunca ocorre na prática, um código numérico que muda de significado entre tabelas. Por isso a EDA neste projeto não foi tratada como uma etapa preliminar descartável, ela foi o que orientou cada decisão de schema da Bronze até a Gold, e continuou sendo revisitada ao longo do desenvolvimento sempre que uma suposição feita cedo demais se mostrava errada ao encontrar o dado real (ver [Lições Aprendidas](#lições-aprendidas) para os casos concretos em que isso aconteceu). A EDA (`notebooks/01_eda_camada_bronze.ipynb`) precedeu toda decisão de schema do pipeline. Os achados mais relevantes:

| #   | Achado                                                                                                                                                                 | Implicação para o Pipeline                                                                                                      |
| --- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| 1   | `dicionario` é uma tabela genérica multi-tabela (`id_tabela` + `nome_coluna` + `chave` → `valor`), não um dicionário por coluna                                        | Join na Silver filtra por `id_tabela` e `nome_coluna` antes de cruzar pela `chave`                                              |
| 2   | `municipio`/`meta_alfabetizacao_municipio` não têm coluna de UF                                                                                                        | Derivar via prefixo de 2 dígitos do código IBGE + crosswalk `ibge_uf_map.csv`                                                   |
| 3   | Nenhuma tabela traz nome de município                                                                                                                                  | Crosswalk `ibge_municipios.csv`, usado só para exibição, nunca como chave de join                                               |
| 4   | `rede`/`serie` são códigos numéricos em `municipio`/`uf`/`alunos`, mas texto nas tabelas de meta                                                                       | Decodificar via dicionário antes de qualquer comparação entre as duas origens                                                   |
| 5   | `alunos` é a maior tabela (3,87M linhas)                                                                                                                               | Mantida bruta na Bronze; **não** processada em Silver/Gold: decisão de escopo                                                   |
| 6   | Nível município cobre só 2023–2024 nos dados reais; UF e Brasil chegam a 2025                                                                                          | Limitação documentada, não estendida artificialmente                                                                            |
| 7   | Código `3` = Municipal é o único presente em 100% das metas municipais                                                                                                 | Filtro obrigatório antes do join de meta no nível município                                                                     |
| 8   | Código `5` = Pública (Estadual+Municipal) corresponde à meta em nível UF, não `3` nem `6`                                                                              | Filtro correspondente no nível UF                                                                                               |
| 9   | **Nem `Federal` (1) nem `Privada` (4) ocorrem em `municipio.csv`/`uf.csv`**, em nenhum ano; `Privada` aparece em só 25 de 3,87M linhas de `alunos.csv`                 | Datasets de desigualdade por rede cobrem só Municipal, Estadual e Pública: Privada removida do schema Gold                      |
| 10  | `nivel_alfabetizacao` existe só em `meta_alfabetizacao_municipio`, sem entrada no dicionário                                                                           | Mantida como número bruto, sem decodificação; escala não documentada pela fonte                                                 |
| 11  | `percentual_participacao` presente nas 3 tabelas de meta                                                                                                               | Usada como sinalizador de confiabilidade estatística na Gold                                                                    |
| 12  | ~48% de nulos em `proporcao_aluno_nivel_0..8` em `uf`/`municipio`                                                                                                      | Provável sigilo estatístico (poucos alunos por célula); mantido nulo, não imputado                                              |
| 13  | 148 municípios com `rede = Municipal` sem meta correspondente (ano único): 242 considerando 2023+2024 juntos                                                           | `LEFT JOIN`/flag `possui_meta`, não `INNER JOIN`: lacuna real de adesão ao programa                                             |
| 14  | RR, AC e DF têm meta pactuada em UF sem resultado real medido em alguns anos; 27 UFs/ano ficam sem resultado em 2025 por ausência estrutural (`uf.csv` não cobre 2025) | Flag `possui_resultado` distinta de `possui_meta`; tratado como achado, não erro                                                |
| 15  | Municípios pequenos podem variar dezenas de pontos percentuais de um ano para o outro (ex: 12,8% → 87,88%)                                                             | Investigado e atribuído a baixo volume amostral, não a erro de coleta: sinalizado como limitação para uso em modelos preditivos |

---

## Decisões Arquiteturais e Trade-offs

### AWS vs. GCP/Azure

A nuvem escolhida foi a **AWS**. Tecnicamente, as três opções permitidas pelo edital (AWS, GCP, Azure) atenderiam bem a este projeto, a arquitetura medalhão com ingestão serverless não depende de nenhum recurso exclusivo de um provedor específico. A decisão real teve um componente prático que vale documentar com transparência: o ambiente disponível para o time é o **AWS Academy Learner Lab**, que fornece créditos de uso já provisionados e prontos para uso imediato nessa nuvem especificamente. Rodar o projeto em GCP ou Azure exigiria criar e financiar uma conta própria (ou buscar créditos educacionais equivalentes em outro provedor), sem a vantagem de a AWS já vir configurada, com role e permissões liberadas para uso acadêmico. Dado o prazo do desafio, essa disponibilidade de crédito pesou tanto quanto qualquer critério técnico na escolha final, e essa é a mesma lógica de custo/tempo disponível que orienta as demais decisões de trade-off documentadas abaixo (por exemplo, Lambda em vez de Glue, Athena em vez de Redshift).

### Batch vs. Streaming: os dois são simulados, por motivos diferentes

A Base dos Dados, fonte oficial deste desafio, não oferece uma API de extração automatizável dentro do escopo e do tempo deste projeto: o acesso disponível é via download manual (console web ou BigQuery). Por isso, **tanto o batch quanto o streaming são simulações**, não só o streaming:

| Aspecto                          | Batch                                                                                                                                                                                                                                                                             | Streaming                                                                                                    |
| -------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Como é simulado                  | Os 9 arquivos foram baixados manualmente da Base dos Dados/IBGE e enviados (upload manual) para o bucket `tech-challenge-fonte-externa-felipe`, que atua como landing zone: em produção, esse upload seria substituído por uma extração automatizada agendada contra a fonte real | Producer gera eventos sintéticos simulando a chegada de uma nova medição, publicados em `streaming/staging/` |
| O que a Lambda simula de verdade | A partir do momento em que o arquivo está na landing zone, a ingestão em si (leitura, particionamento, gravação na Bronze) é real, roda em Lambda, participa do gatilho de evento, gera log: só a etapa de "chegada do arquivo na landing zone" é manual em vez de automatizada   | Ingestão incremental "quase em tempo real", como pedido pelo edital                                          |
| Por quê                          | Reproduzir com fidelidade uma extração automatizada de uma fonte que não oferece API de pull dentro do prazo do projeto seria desproporcional ao valor demonstrado: o objetivo é mostrar que o time entende quando usar cada abordagem, não construir um scraper de produção      | Kafka/fila gerenciada real não são viáveis nem necessários para o volume de demonstração                     |

Essa distinção é importante para a avaliação: a arquitetura de ingestão (Lambda, particionamento, gatilho de evento, tratamento de erro) é real e funcional; o que é simulado é exclusivamente a origem do dado: tanto o "arquivo chegando" (batch) quanto o "evento chegando" (streaming).

### Kafka vs. SQS vs. Staging simples no S3 (escolhida)

O producer grava eventos JSON diretamente numa pasta de staging no bucket de origem; o consumer é acionado por gatilho **S3 PUT** (não por polling), lê o evento, grava na Bronze e apaga o original do staging. Kafka foi descartado por ser overhead operacional desproporcional ao volume simulado deste projeto; SQS foi avaliado como intermediário (desacoplamento, replay via DLQ), mas o ganho não compensa a complexidade de configuração adicional para o volume atual. O trade-off aceito: sem garantias nativas de replay/ordenação, adequado para o escopo de simulação, documentado como ponto de evolução caso o projeto crescesse para produção real.

### Glue/Spark vs. Lambda + Pandas (escolhida)

Um Glue Job Spark tem tempo de inicialização de cluster de 1–2 minutos, cobrado em blocos de DPU, mesmo para processar poucos KB. As 9 tabelas deste projeto são processadas por completo em segundos via Lambda + Pandas. Uma migração para Glue foi avaliada explicitamente durante o desenvolvimento (ver seção FinOps) e descartada pelo mesmo motivo. Glue passaria a ser a escolha correta se o volume de dados crescesse em ordens de grandeza: por exemplo, se `alunos` (atualmente fora de escopo) passasse a ser processada em Silver/Gold.

### Athena vs. Redshift/Redshift Serverless

Athena foi escolhido para consulta da camada Gold: schema-on-read direto sobre Parquet no S3, sem infraestrutura para provisionar ou manter, cobrança por dado escaneado (frações de centavo dado o volume do projeto). Redshift (mesmo Serverless) foi avaliado e descartado: é uma ferramenta desenhada para volumes bem maiores com múltiplos usuários consultando repetidamente; mesmo a variante Serverless cobra capacidade mínima em repouso, diferente do Athena, que é genuinamente zero-custo quando ninguém consulta. Configurar Redshift também exigiria rede (VPC) e esbarraria nas mesmas restrições de IAM do Learner Lab.

### Athena: DDL manual vs. Glue Crawler

O schema das 5 tabelas Gold é pequeno, estável e definido pelo próprio pipeline: `CREATE EXTERNAL TABLE` manual (`src/gold/athena_ddl_setup.sql`) evita o custo/complexidade de um Crawler automático, cuja principal vantagem (descoberta automática de schema) não se aplica aqui.

### Particionamento: nem sempre mais é melhor

Particionamento segue o **valor real da coluna `ano`** dos dados, não a data de ingestão: otimiza consultas analíticas reais, que filtram por ano de referência, não por quando o dado chegou ao lake. Mas particionamento não foi aplicado uniformemente: `meta_alfabetizacao_brasil` (1 linha/ano) e `meta_alfabetizacao_municipio` (não sub-particionada por UF) ficaram deliberadamente sem partição adicional: criar uma pasta por ano para um arquivo de 1 linha é o "small file problem" na prática, onde o overhead de metadado do Parquet supera o ganho de performance de leitura seletiva.

### Preservação de histórico na Bronze

Cada execução do batch grava um arquivo **novo** (nome com timestamp de ingestão), em vez de sobrescrever a partição: atende ao requisito explícito do edital de "histórico completo preservado" na camada Bronze, permitindo auditar valores anteriores caso a fonte republique dados corrigidos.

### Escopo da tabela `alunos`

Mantida bruta na Bronze (evidência de ingestão completa), não processada em Silver/Gold. Volume (3,87M linhas) e complexidade de nível-aluno excedem o tempo disponível; os indicadores de negócio exigidos pelo edital são plenamente atendidos no nível município/UF/Brasil.

### Tabelas de meta como base própria, não como alvo de join

As três tabelas de meta (`meta_alfabetizacao_{municipio,uf,brasil}`) já trazem `taxa_alfabetizacao` (resultado real) e as metas na mesma linha: não são apenas metas a serem casadas com uma tabela de resultado separada. A Gold usa essas tabelas diretamente como base para as comparações meta-vs-resultado; o join com `municipio`/`uf` bruto existe só para **enriquecer** com `media_portugues`, que não existe nas tabelas de meta: não para preencher lacuna de resultado.

### Data Lake vs. Data Warehouse

A solução foi construída como **data lake** (S3 + Parquet + Athena), não como data warehouse (Redshift, BigQuery, Snowflake). A diferença central: um data warehouse exige carga prévia dos dados numa estrutura otimizada e proprietária antes de qualquer consulta ser possível; um data lake consulta o dado onde ele já está, sem etapa de carga. Para este projeto, essa diferença favorece claramente o lake: o volume é pequeno, o schema muda conforme o pipeline evolui (como aconteceu várias vezes ao longo do desenvolvimento, ver [Lições Aprendidas](#lições-aprendidas)), e não há um time fixo de analistas fazendo consultas repetidas e complexas que justificasse o investimento de estrutura de um warehouse. Data lake também mantém as três camadas (Bronze/Silver/Gold) com naturalidade, já que cada camada é só uma pasta com arquivos, sem exigir schema fixo antecipado. Data warehouse voltaria a fazer sentido no mesmo cenário em que Redshift voltaria a fazer sentido (ver acima): volume bem maior e consultas repetidas por múltiplos usuários.

### Custo vs. Performance

Cada decisão de ferramenta deste projeto foi resolvida a favor de custo mínimo em vez de performance máxima, porque o volume de dados não exige o contrário: Lambda em vez de Glue (segundos de execução vs. minutos de cold start de cluster), Athena em vez de Redshift (pay-per-query vs. capacidade provisionada), staging simples em vez de fila gerenciada (sem custo de infraestrutura extra vs. desacoplamento/replay que este volume não precisa). Em nenhum desses casos a alternativa mais cara traria ganho de performance perceptível dado o tamanho real dos dados, o que tornaria o custo extra puro desperdício. O único ponto em que performance pesou mais que o custo mínimo absoluto foi o dimensionamento de memória da Lambda de ingestão batch (2048 MB, para evitar `OutOfMemory` ao processar `alunos.csv`): ali, a alternativa mais barata (menos memória) simplesmente não completaria a execução, então não era uma troca real de custo por performance, era um piso técnico necessário.

### Rede Privada removida do schema de desigualdade

Descoberta tardia (já com a Gold construída): a rede Privada nunca ocorre em `municipio.csv`/`uf.csv`, apenas 25 vezes em 3,87 milhões de linhas de `alunos.csv` (fora de escopo). O schema de `desigualdade_por_rede_*` foi corrigido para conter só Municipal, Estadual e Pública, com o gap calculado como `Municipal - Estadual` em vez de `Privada - Pública`. Ver [Lições Aprendidas](#lições-aprendidas).

---

## Estrutura do Repositório

```
FIAP-Tech-Challenge-Fase-2-Pipeline-ETL/
├── .gitignore                         # *.csv, *.parquet, *.json, __pycache__/, .env
├── README.md
├── requirements.txt                   # boto3, pandas, pyarrow, s3fs, jupyter
├── notebooks/
│   └── 01_eda_camada_bronze.ipynb
├── src/
│   ├── bronze/
│   │   ├── batch/
│   │   │   └── ingestao_batch.py
│   │   └── streaming/
│   │       ├── streaming_producer.py
│   │       └── streaming_consumer.py
│   ├── silver/
│   │   ├── transformacao_silver.py
│   │   └── qualidade_dados.py
│   └── gold/
│       ├── construcao_gold.py
│       ├── athena_ddl_setup.sql
│       └── athena_dql_exemplos.sql
├── scripts_auxiliares/                # ferramentas de investigacao pontual, fora do pipeline em producao
│   ├── diagnosticar_lacuna_uf.py      # investiga por ano a lacuna de resultado sem correspondencia em uf
│   └── testar_upsert_streaming.py     # valida isoladamente o upsert de streaming da silver
└── docs/
    ├── diagrama_arquitetura.png
    ├── Pipeline-Dados-Alfabetizacao-Brasil.pptx  # slides usados na gravação do video executivo
    ├── link_video_executivo.txt        # link para o video executivo (YouTube)
    └── link_video_evidencia.txt        # link para o video mostrando a pipeline rodando no console AWS
```

---

## Camadas do Pipeline

### Bronze: Batch

**Script:** `src/bronze/batch/ingestao_batch.py`

- Whitelist fechada dos 9 arquivos esperados: nunca varre o bucket de origem sem filtro.
- Particiona dinamicamente: se a tabela tem coluna `ano`, particiona por ela; senão (dicionário e os dois crosswalks), grava sem partição.
- Cada execução grava arquivo novo com timestamp de ingestão (preserva histórico, não sobrescreve).
- Coluna de metadado `data_ingestao_datalake` adicionada a cada registro.
- Formato de handler Lambda, testado localmente antes do deploy.

### Bronze: Streaming (simulação)

**Scripts:** `src/bronze/streaming/streaming_producer.py`, `streaming_consumer.py`

- **Producer:** amostra códigos reais de município (via `ibge_municipios.csv`) e publica eventos JSON (`{evento_id, id_municipio, ano, rede, taxa_alfabetizacao_atualizada, timestamp_evento}`) na pasta `streaming/staging/` do bucket de **origem**: preserva o papel de landing zone, sem misturar dado transiente com o data lake.
- **Consumer:** acionado por gatilho S3 PUT no staging (não por polling), processa apenas o objeto específico do evento (evita corrida entre invocações concorrentes), grava em `bronze/streaming/ano={ano}/` e remove o original do staging.
- Producer agendado via EventBridge (`rate(5 minutes)`), simulando chegada periódica de eventos.

### Silver

**Script:** `src/silver/transformacao_silver.py`

Dois modos de operação, decididos pela origem do evento que aciona a Lambda:

- **Batch** (`bronze/batch/`): reconstrói a partição inteira (tabela, ano) a partir do arquivo mais recente da Bronze: decodifica `rede`/`serie` via dicionário, deriva `uf` (crosswalk) e `nome_municipio` (crosswalk) para a tabela `municipio`.
- **Streaming** (`bronze/streaming/`): upsert pontual, localiza a linha por `id_municipio` **e** `rede` (crítico: sem o filtro de rede, um evento sobrescreveria indevidamente linhas de outras redes do mesmo município, bug capturado e corrigido durante teste manual com `scripts_auxiliares/testar_upsert_streaming.py`), atualiza só `taxa_alfabetizacao`, marca `origem_ultimo_valor = 'streaming'`.

Tabelas produzidas: `municipio` (partição `ano`+`uf`), `uf` (partição `ano`), `meta_alfabetizacao_municipio` (partição `ano`), `meta_alfabetizacao_uf` (partição `ano`), `meta_alfabetizacao_brasil` (arquivo único, sem partição: mesclado a cada execução para não perder anos anteriores). `alunos`, `dicionario` e os crosswalks não geram output próprio: a whitelist de tabelas com output (`TABELAS_SILVER`) é verificada de forma explícita, evitando que qualquer arquivo fora do escopo seja processado por engano.

### Gold

**Script:** `src/gold/construcao_gold.py`

Cinco tabelas, cada uma no grão correto (nunca misturando município + UF + Brasil numa tabela só):

| Tabela                            | Grão                   | Base                                  | Enriquecimento                                                                    |
| --------------------------------- | ---------------------- | ------------------------------------- | --------------------------------------------------------------------------------- |
| `indicador_vs_meta_municipal`     | `id_municipio` + `ano` | `silver/meta_alfabetizacao_municipio` | `media_portugues`, `nome_municipio`, `uf` via `silver/municipio` (rede Municipal) |
| `indicador_vs_meta_uf`            | `sigla_uf` + `ano`     | `silver/meta_alfabetizacao_uf`        | `media_portugues` via `silver/uf` (rede Pública)                                  |
| `indicador_vs_meta_brasil`        | `ano`                  | `silver/meta_alfabetizacao_brasil`    | Nenhum (não existe `brasil.csv` bruto)                                            |
| `desigualdade_por_rede_municipio` | `id_municipio` + `ano` | `silver/municipio`, pivotada por rede | -                                                                                 |
| `desigualdade_por_rede_uf`        | `sigla_uf` + `ano`     | `silver/uf`, pivotada por rede        | -                                                                                 |

Colunas calculadas nas tabelas de indicador: `meta_alfabetizacao` (leitura diagonal: o valor de `meta_alfabetizacao_{ano}` correspondente ao próprio `ano` da linha; nula em 2023, ano-base sem meta pactuada), `possui_resultado`, `possui_meta`, `baixa_confiabilidade` (`percentual_participacao < 85`), `deficit_meta`. Nas tabelas de desigualdade: `taxa_alfabetizacao_{municipal,estadual,publica}`, `gap_taxa_alfabetizacao_municipal_estadual` e os mesmos para `media_portugues`.

Cada evento de Silver aciona a reconstrução só das tabelas Gold que dependem daquela tabela de origem (ex: um evento em `silver/municipio` reconstrói `indicador_vs_meta_municipal` **e** `desigualdade_por_rede_municipio`, não as outras três).

### Athena

- `athena_ddl_setup.sql`: `CREATE DATABASE` + `CREATE EXTERNAL TABLE` para as 5 tabelas + `MSCK REPAIR TABLE` para descoberta de partição.
- `athena_dql_exemplos.sql`: uma consulta de validação cobrindo as 5 tabelas de uma vez, mais uma consulta analítica por tabela, ligada aos três usos pedidos pelo edital (política pública, desigualdade educacional, predição).

---

## Qualidade de Dados

**Script:** `src/silver/qualidade_dados.py`: roda um conjunto fixo de checagens sobre a Silver, sem interromper o pipeline em caso de achado (os achados aqui são lacunas de cobertura conhecidas da fonte, não erros a corrigir):

1. **Duplicidade** de chave por tabela (`0` em todas, confirmado).
2. **Nulos** em colunas críticas: `taxa_alfabetizacao` nula em 120 linhas de `meta_alfabetizacao_municipio` (2023) e 4 de `meta_alfabetizacao_uf`, ambos confirmados contra o CSV original antes de aceitar como achado válido, não bug.
3. **Cobertura de decodificação**: nenhum código de `rede`/`serie` sem tradução após o merge com o dicionário.
4. **Integridade referencial** entre resultado e meta, com cast defensivo de tipo antes da comparação (ver [Lições Aprendidas](#lições-aprendidas)): 242 municípios sem meta correspondente (2023+2024), 50 sem resultado; 32 casos de UF sem resultado (27 por ausência estrutural de 2025 em `uf.csv`, 5 reais em RR/AC/DF, quebra por ano em `scripts_auxiliares/diagnosticar_lacuna_uf.py`).

---

## FinOps: Otimização de Custos

| Prática                                                             | Impacto                                                                                                             |
| ------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------- |
| **Parquet** (Silver/Gold)                                           | Compressão colunar reduz tamanho de armazenamento; Athena lê só as colunas necessárias                              |
| **Particionamento seletivo** por `ano`/`uf`                         | Reduz volume de scan no Athena: aplicado só onde o volume justifica (ver decisão sobre `meta_alfabetizacao_brasil`) |
| **Arquitetura serverless** (Lambda)                                 | Sem custo de infraestrutura ociosa; paga-se por execução                                                            |
| **Athena pay-per-query**                                            | Sem servidor provisionado; escolhido em vez de Redshift/Redshift Serverless pelo mesmo motivo                       |
| **Dimensionamento de memória validado com dado real**, não estimado | Ver abaixo                                                                                                          |

### Como os custos são controlados

A AWS oferece ferramentas nativas de controle de custo (**Cost Explorer**, **AWS Budgets**, **Cost Anomaly Detection**), mas no **AWS Academy Learner Lab** o acesso a essas ferramentas é restrito: a conta é gerenciada pela própria Academy, com um teto de créditos fixo por laboratório, e o estudante não tem acesso ao console de billing/cost management como teria numa conta própria (a mesma restrição de IAM que já impede criar usuários e roles também se estende ao gerenciamento de custo). Isso significa que o controle de custo neste projeto não pôde se apoiar num dashboard de billing nativo, e precisou vir de outro lugar: da própria evidência de execução que cada Lambda já gera.

Toda invocação de Lambda registra, no CloudWatch, uma linha `REPORT` com `Billed Duration` (tempo efetivamente cobrado, arredondado para cima em milissegundos) e `Memory Size`/`Max Memory Used`. Como o custo de Lambda é calculado por **GB-segundo** (memória alocada × duração cobrada), esses dois números, já disponíveis de graça em todo log de execução, são suficientes para estimar o custo real de cada função sem precisar de nenhuma ferramenta de billing:

```
custo_estimado_por_execucao = (memoria_gb × billed_duration_segundos) × preco_por_gb_segundo
```

Foi exatamente esse dado (`Billed Duration` e `Max Memory Used` do `REPORT`) que orientou o ajuste de memória documentado na seção seguinte: a decisão de reduzir a Lambda de transformação da Silver de 1024 MB para 512 MB não foi um palpite, foi baseada no pico real de uso medido em execuções sucessivas. Em produção, esses mesmos logs poderiam alimentar um dashboard de FinOps automatizado (ex: exportando métricas do CloudWatch para um dataset próprio, ou usando CloudWatch Metrics Insights), o que fica registrado aqui como próximo passo natural, não implementado neste ciclo por estar fora do escopo de tempo do desafio.

### Caso real de dimensionamento de memória

A Lambda de ingestão batch (que processa `alunos.csv`, 3,87M linhas, dentro do escopo legítimo da Bronze) precisou de **2048 MB** após um `OutOfMemory` em 512 MB. Já a Lambda de transformação da Silver, após a correção do bug que fazia `alunos` vazar para essa camada (ver Lições Aprendidas), teve seu pico real de uso medido em **~228–240 MB** processando o caso mais pesado de verdade (`municipio`, múltiplas partições de UF): memória configurada reduzida de 1024 MB para **512 MB**, mantendo mais que o dobro de margem de segurança, sem impacto perceptível na duração.

### Por que não Glue/Spark, por que não Redshift

Ver seção de [Decisões Arquiteturais](#decisões-arquiteturais-e-trade-offs): ambas avaliadas e descartadas com justificativa de custo/volume, não por desconhecimento das ferramentas.

---

## Monitoramento

Implementado via **logs estruturados** (`logging`, não `print` solto) em cada Lambda, capturados nativamente pelo CloudWatch:

- Cada função retorna um resumo estruturado (`status`, itens processados, falhas): visível tanto no retorno da invocação quanto no CloudWatch Logs.
- Falha em um item (um arquivo, um evento) não interrompe o processamento dos demais: captura por `try/except` individual, com resumo de sucesso/falha ao final.
- O script `qualidade_dados.py` funciona como uma auditoria executável a qualquer momento, gerando evidência de conformidade sem depender de ferramenta externa de observabilidade.
- Execução orientada a evento (gatilhos S3 PUT em cadeia: Bronze → Silver → Gold) foi validada de ponta a ponta, com logs de cada camada mostrando o encadeamento automático sem intervenção manual.

**Limitação reconhecida:** não foi implementado monitoramento avançado (CloudWatch Alarms, dashboards, alertas). Em produção, seria prioritário configurar alarmes de falha de Lambda e métricas customizadas de qualidade de dado: fora do escopo de tempo deste desafio.

---

## Aplicação em Inteligência Artificial

A camada Gold entrega dados prontos, no grão certo, para quatro frentes de aplicação em IA.

### Modelos de predição

- **Alvo em potencial:** classificação binária "município atinge a meta de 2030?" ou regressão da trajetória de `taxa_alfabetizacao` até 2030.
- **Algoritmos candidatos:** para o baseline, uma regressão logística ou uma árvore de decisão simples (interpretabilidade importa aqui, já que o consumidor final é um gestor público, não um time de ML); para uma segunda iteração, Random Forest ou Gradient Boosting (XGBoost/LightGBM), que lidam bem com o volume de features relativamente pequeno e com dados faltantes sem exigir imputação agressiva.
- **Limitação honesta:** o nível município só tem 2 anos de série real (2023-2024), insuficiente para um modelo robusto de série temporal. Documentado como próximo passo natural quando mais anos de coleta estiverem disponíveis, não implementado neste ciclo.
- **Features prontas na Gold**, sem trabalho adicional: `deficit_meta` (distância corrente até a meta), os `gap_*_municipal_estadual` (desigualdade entre redes no mesmo território), `percentual_participacao`/`baixa_confiabilidade` (para ponderar ou filtrar casos estatisticamente frágeis), `media_portugues` (proficiência, correlacionada mas não idêntica à taxa de alfabetização).
- **Cuidado identificado na EDA:** municípios pequenos, com resultado volátil entre anos consecutivos (achado #15 da EDA), tendem a ser ruído por baixo volume amostral. Um modelo real deveria ponderar por tamanho de amostra (ex: número de alunos avaliados, se disponível) para não deixar esses casos dominarem o treinamento ou a métrica de erro.

### Clusterização de perfis de vulnerabilidade

Usando as colunas de resultado, meta e desigualdade por rede já disponíveis na Gold como espaço de features, um algoritmo de clusterização (K-Means, ou hierárquico para explorar o número de grupos antes de fixar) pode agrupar municípios em perfis (ex: "alta desigualdade Municipal vs. Estadual e meta distante", "resultado consistente mas participação baixa", "próximo da meta e alta confiabilidade"). Isso atende diretamente ao pedido do edital de "clusters de vulnerabilidade educacional" e é mais simples de implementar que um modelo preditivo supervisionado, já que não depende de rótulo/alvo histórico.

### Enriquecimento com dados externos (próximo passo)

O edital sugere integrar dados de contexto socioeconômico e territorial (Censo Escolar/INEP, IBGE, Atlas do Desenvolvimento Humano, Cadastro Único). Nenhuma dessas fontes foi integrada neste ciclo, mas a arquitetura já comporta isso sem redesenho: bastaria adicionar os novos arquivos à whitelist da Bronze, um crosswalk por `id_municipio` (mesmo padrão já usado para UF e nome de município) e um novo enriquecimento na Gold. O ganho esperado é analítico direto, features como renda média, infraestrutura escolar ou IDH municipal são candidatos fortes a explicar variação de resultado que os dados atuais, sozinhos, não capturam.

### Retreinamento contínuo via streaming

Como a arquitetura já ingere atualizações incrementais via streaming (mesmo que hoje simuladas), o mesmo gatilho que atualiza a Silver e a Gold poderia, em produção, disparar reavaliação periódica de um modelo já treinado, sem esperar o próximo ciclo de batch completo. Não implementado neste projeto, mas é uma extensão natural da arquitetura orientada a evento já construída.

### Análise de desigualdade educacional

As tabelas `desigualdade_por_rede_{municipio,uf}` já entregam o gap Municipal vs. Estadual pré-calculado, prontas para ranking e mapa, sem necessidade de nenhuma transformação adicional em quem consome via Athena/BI.

### Políticas públicas baseadas em dados

As flags `possui_resultado`/`possui_meta` (nas tabelas de indicador) tornam lacunas de cobertura (municípios sem meta pactuada, UFs sem resultado medido) em achados explícitos e acionáveis, não perdas silenciosas de linha em um join. Isso permite responder diretamente "quais territórios precisam de atenção prioritária de coleta de dado, antes mesmo de falar de desempenho".

---

## Como Executar Localmente

### Pré-requisitos

```bash
pip install -r requirements.txt
```

AWS CLI configurado com credenciais válidas (`~/.aws/credentials`): ver [Restrições do Ambiente](#restrições-do-ambiente) sobre credenciais temporárias do Learner Lab.

### Ordem de execução

```bash
# 1. Ingestão batch (fonte externa → Bronze)
python src/bronze/batch/ingestao_batch.py

# 2/3. Streaming (rodar em terminais separados para simular producer/consumer independentes)
python src/bronze/streaming/streaming_producer.py
python src/bronze/streaming/streaming_consumer.py

# 4. Transformação Silver (Bronze → Silver)
python src/silver/transformacao_silver.py

# 5. Qualidade de dados (gera relatório)
python src/silver/qualidade_dados.py

# 6. Construção Gold (Silver → Gold)
python src/gold/construcao_gold.py

# 7. Athena: rodar no Query Editor do console AWS
# athena_ddl_setup.sql primeiro, depois athena_dql_exemplos.sql
```

Cada script de transformação (`transformacao_silver.py`, `construcao_gold.py`) aceita ser chamado com `lambda_handler(None, None)` para reconstrução completa (uso local/teste) ou com um evento `{"Records": [...]}` simulando um gatilho S3, replicando o comportamento real da AWS sem precisar configurar infraestrutura para testar a lógica.

---

## Deploy na AWS

Ambiente: **AWS Academy Learner Lab**, role `LabRole` (única disponível, permissões já provisionadas para S3/Lambda). Todas as Lambdas usam a layer gerenciada `AWSSDKPandas-PythonXX` (pandas, numpy, pyarrow, boto3 nativo no runtime).

| Lambda                                  | Script                    | Handler                               | Memória | Timeout | Gatilho                                                                       |
| --------------------------------------- | ------------------------- | ------------------------------------- | ------- | ------- | ----------------------------------------------------------------------------- |
| `tech-challenge-ingestao-bronze`        | `ingestao_batch.py`       | `ingestao_batch.lambda_handler`       | 2048 MB | ~1 min  | EventBridge (configurado para teste, atualmente desativado)                   |
| `tech-challenge-streaming-producer`     | `streaming_producer.py`   | `streaming_producer.lambda_handler`   | 256 MB  | 15 s    | EventBridge `rate(5 minutes)` (configurado para teste, atualmente desativado) |
| `tech-challenge-streaming-consumer`     | `streaming_consumer.py`   | `streaming_consumer.lambda_handler`   | 128 MB  | 30 s    | S3 PUT em `streaming/staging/` (bucket de origem)                             |
| `tech-challenge-transformacao-silver`   | `transformacao_silver.py` | `transformacao_silver.lambda_handler` | 512 MB  | 2 min   | S3 PUT em `bronze/batch/` **e** `bronze/streaming/` (bucket do data lake)     |
| `tech-challenge-qualidade-dados-silver` | `qualidade_dados.py`      | `qualidade_dados.lambda_handler`      | 512 MB  | 1 min   | Manual / EventBridge opcional                                                 |
| `tech-challenge-construcao-gold`        | `construcao_gold.py`      | `construcao_gold.lambda_handler`      | 512 MB  | 2 min   | S3 PUT em `silver/` (bucket do data lake)                                     |

**Buckets:** `tech-challenge-fonte-externa-felipe` (landing zone: arquivos batch + staging de streaming) e `tech-challenge-datalake-felipe` (Bronze/Silver/Gold).

**Sobre os agendamentos do EventBridge:** foram configurados apenas durante a fase de testes (`rate(5 minutes)` no producer, agendamento pontual na ingestão batch) para gerar evidência de execução automática e encadeada entre as camadas: ver `docs/link_video_evidencia.txt`. Ambos estão atualmente **desativados**, para não gerar execuções desnecessárias fora da janela de testes; as Lambdas continuam funcionais e podem ser invocadas manualmente ou reagendadas a qualquer momento.

---

## Restrições do Ambiente

| Restrição                                                                               | Impacto                                                     | Mitigação                                                                                                                           |
| --------------------------------------------------------------------------------------- | ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| Credenciais temporárias do AWS Academy Learner Lab (expiram em horas)                   | Scripts locais podem falhar em execuções longas             | Recolar credenciais em `~/.aws/credentials` a partir de "AWS Details" no lab                                                        |
| Sem permissão de IAM (não é possível criar usuários/roles)                              | Impede o fluxo tradicional de criação de usuário/access key | Uso da role padrão do lab (`LabRole`), já provisionada                                                                              |
| Infraestrutura provisionada manualmente via console, sem IaC (Terraform/CloudFormation) | Sem reprodutibilidade automatizada                          | Aceito como limitação de escopo/tempo; este README documenta os passos manuais como guia de recriação                               |
| Infraestrutura não mantida ativa após a entrega                                         | Avaliadores não acessam a conta AWS diretamente             | Evidência via `docs/link_video_evidencia.txt` (pipeline rodando no console AWS) e `docs/link_video_executivo.txt` (vídeo executivo) |

---

## Lições Aprendidas

Registradas deliberadamente: o processo de descoberta e correção é, na visão deste time, tão relevante quanto o resultado final:

- **Categoria prevista no dicionário ≠ categoria presente no dado.** O schema inicial da Gold assumia que Federal e Privada tinham resultado medido (baseado na existência da categoria no dicionário), até a validação com `value_counts()` revelar zero ocorrências reais em `municipio.csv`/`uf.csv`. Corrigido antes da entrega, com o gap recalculado para Municipal vs. Estadual. Lição de processo: qualquer schema baseado em enumeração de categorias merece validação contra dado real antes de ser tratado como definitivo, não só as categorias que a tarefa imediata exige checar.
- **Incompatibilidade de tipo pode silenciar um join inteiro.** Um bug de tipo (`id_municipio` como `string` numa tabela e `int64` noutra) fez a checagem de integridade referencial reportar praticamente 100% de não-correspondência: não porque o dado estivesse errado, mas porque `"123" != 123` em uma comparação de tuplas. Corrigido padronizando o tipo na origem da Silver, com um cast defensivo adicional no script de qualidade.
- **S3 URL-encoda chaves de objeto em notificações de evento.** `=` vira `%3D` no payload do evento S3, causando `NoSuchKey` em chaves particionadas (`ano=2024`). Corrigido com `urllib.parse.unquote_plus` antes de qualquer leitura baseada em chave de evento.
- **Uma condição de filtro por lista negra deixou passar o que não devia.** A Lambda de Silver processava qualquer tabela que não fosse explicitamente "de apoio": o que permitiu que `alunos` (fora de escopo) fosse processada por engano, quase esgotando a memória alocada. Corrigido trocando para lista branca (só processa o que está explicitamente definido como tabela com output).
- **Achado estatístico chamativo não é necessariamente achado real.** Um salto de alfabetização de +75 pontos percentuais em municípios do Maranhão parecia, à primeira vista, um case de sucesso de política pública. A comparação com o agregado estadual (que mal se moveu) revelou que era ruído de amostra pequena em municípios individuais, não um efeito real e disseminado. Evitado por hábito de validar hipóteses "boas demais" com uma segunda fonte antes de aceitar a conclusão mais interessante.

---

## Nota Pessoal sobre o Prazo de Entrega

Um recado direto e sincero antes de encerrar.

Entreguei o link deste repositório dentro do prazo, até o dia 14/07, no portal da FIAP, como exigido. Mas quero ser honesto: parte dos commits deste repositório tem data posterior a essa entrega. Isso aconteceu porque não consegui montar um grupo a tempo (a maioria das turmas já estava com vagas fechadas quando fui procurar) e acabei desenvolvendo o projeto inteiro sozinho, e nas últimas semanas o volume de trabalho na minha rotina profissional também pesou bastante contra o tempo que eu conseguia dedicar ao desafio.

Sei que isso pode custar pontos na avaliação, e entendo se for o caso. Não é uma reclamação nem uma tentativa de justificar o injustificável, é só um contexto real que prefiro deixar registrado com transparência a esconder.

O que peço, com o maior respeito possível, é que a avaliação considere o projeto como um todo: a profundidade das decisões tomadas, a quantidade de validações feitas contra dado real antes de aceitar qualquer achado, os bugs encontrados e corrigidos ao longo do caminho, e o cuidado que tentei colocar em cada camada do pipeline, mesmo trabalhando sozinho e com o tempo apertado. Fico à disposição para qualquer esclarecimento adicional que for necessário.

Obrigado pela compreensão.
