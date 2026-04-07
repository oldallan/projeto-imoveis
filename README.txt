PROJETO: COLETA E CONSOLIDACAO DE DADOS IMOBILIARIOS

VISAO GERAL
Este projeto executa um pipeline de coleta, padronizacao e consolidacao de anuncios imobiliarios de venda e aluguel. Os dados sao extraidos de OLX, QuintoAndar e Lopes, transformados para um schema unico e incorporados em uma base historica incremental, pronta para analise.

COMO FUNCIONA
O fluxo principal e dividido em 3 etapas:

1. collect_general_listings
Executa os scrapers de cada fonte e gera a coleta bruta diaria em arquivos separados por data.

2. build_daily_snapshot
Normaliza os campos, remove duplicidades e gera o snapshot diario consolidado.

3. update_historical_store
Aplica upsert no consolidado historico, preservando uma base acumulada e atualizada ao longo do tempo.

FONTES DE DADOS
- OLX
- QuintoAndar
- Lopes

SAIDAS GERADAS
- raw/<data>/
  Armazena os arquivos brutos coletados por fonte e tipo de negocio.

- processed/<data>/
  Armazena o snapshot diario consolidado da execucao.

- processed/
  Mantem a base consolidada historica mais recente.

- artifacts/<data>/
  Guarda manifests, metricas e informacoes de execucao de cada etapa do pipeline.

COMO EXECUTAR
Para rodar o pipeline completo:

python cli.py run-all --date DD-MM-YYYY

Tambem e possivel executar etapas isoladas com:

python cli.py run-stage <stage_name> --date DD-MM-YYYY

OBSERVACOES
- O projeto processa os dados por data de execucao, gerando snapshots separados por dia.
- A pasta processed/ concentra a versao consolidada mais recente da base historica.
- Os manifests em artifacts/<data>/ permitem auditar volumes, duracoes e status de cada etapa.

OBJETIVO
Transformar dados dispersos de anuncios imobiliarios em uma base historica incremental, padronizada e pronta para exploracao e analise.
