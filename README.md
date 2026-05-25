# Projeto Imoveis

Pipeline de coleta, normalizacao, deduplicacao e consolidacao historica de
anuncios imobiliarios de venda e aluguel. O projeto coleta dados de OLX,
QuintoAndar e Lopes, transforma os registros para um schema canonico e atualiza
uma base historica incremental pronta para analise.

## Visao Geral

O fluxo atual separa a coleta em duas fases:

1. `collect_discovery`: descobre URLs de anuncios por fonte.
2. `collect_listings`: visita as URLs descobertas e coleta os dados completos.
3. `build_daily_snapshot`: normaliza, enriquece, deduplica e gera o snapshot do dia.
4. `update_historical_store`: aplica upsert no historico consolidado.

As saidas de dados ficam em `raw/`, `processed/`, `artifacts/` e `logs/`. Essas
pastas sao geradas localmente e nao sao versionadas no Git.

## Fontes Suportadas

| Fonte | Discovery | Listings | Regra incremental |
| --- | --- | --- | --- |
| OLX | `scrapers/olx_discovery.py` | `scrapers/olx_listings.py` | anuncios novos ou com preco alterado |
| Lopes | `scrapers/lopes_discovery.py` | `scrapers/lopes_listings.py` | anuncios novos ou com `lastmod` alterado |
| QuintoAndar | `scrapers/quinto_discovery.py` | `scrapers/quinto_listings.py` | anuncios novos ou com `lastmod` alterado |

As fontes sao registradas em `scrapers/registry.py`.

## Instalacao

Requisitos principais:

- Python 3.13 ou compativel com as dependencias do projeto
- `pip`
- Acesso de rede para os scrapers

Instale as dependencias:

```powershell
python -m pip install -r requirements.txt
```

No ambiente usado neste projeto, o Python do Anaconda tambem funciona:

```powershell
C:\ProgramData\anaconda3\python.exe -m pip install -r requirements.txt
```

## Como Executar

Listar stages disponiveis:

```powershell
python cli.py list-stages
```

Rodar o pipeline completo para uma data:

```powershell
python cli.py run-all --date DD-MM-YYYY
```

Rodar com logs mais detalhados dos scrapers:

```powershell
python cli.py run-all --date DD-MM-YYYY --verbose
```

Forcar novo discovery mesmo quando ja existe manifesto de sucesso:

```powershell
python cli.py run-all --date DD-MM-YYYY --force-discovery
```

Retomar o pipeline a partir de uma etapa:

```powershell
python cli.py run-all --date DD-MM-YYYY --from-stage collect_listings
```

Executar uma etapa isolada:

```powershell
python cli.py run-stage collect_discovery --date DD-MM-YYYY
python cli.py run-stage collect_listings --date DD-MM-YYYY
python cli.py run-stage build_daily_snapshot --date DD-MM-YYYY
python cli.py run-stage update_historical_store --date DD-MM-YYYY
```

Limitar uma etapa isolada a fontes especificas:

```powershell
python cli.py run-stage collect_discovery --date DD-MM-YYYY --sources olx lopes
python cli.py run-stage collect_listings --date DD-MM-YYYY --sources olx lopes
```

Quando `--sources` e usado, manifests e outputs escopados podem ficar em
subpastas `sources__<fonte>`.

## Estrutura de Saida

```text
raw/<data>/<fonte>/
  <fonte>_discovery.csv
  <fonte>_discovery.parquet
  <fonte>_listings.csv
  <fonte>_listings.parquet

processed/<data>/
  listings_unificados.parquet
  listings_unificados.csv
  properties_unified.parquet
  properties_unified.csv
  property_listing_link.parquet

processed/
  listings_unificados.parquet
  properties_unified.parquet
  properties_unified.csv
  property_listing_link.parquet

artifacts/<data>/
  pipeline_run.json
  <stage>/manifest.json
  collect_listings/<fonte>/...

logs/<data>/
  <stage>.log
```

`processed/<data>/` representa o snapshot diario. `processed/` na raiz contem a
versao historica mais recente.

## Comportamento Incremental

- `collect_discovery` usa regras por fonte para reduzir a coleta ao que e novo
  ou alterado.
- `run-all` reutiliza um manifesto de `collect_discovery` bem-sucedido quando
  ele ja existe para a data, a menos que `--force-discovery` seja informado.
- Se o discovery terminar sem novos links, o pipeline encerra cedo com
  `stop_reason = no_new_links_after_discovery`.
- `collect_listings` usa estado de resume em `artifacts/<data>/collect_listings`
  para evitar repetir trabalho ja concluido.
- Stages que dependem de etapas anteriores leem o manifesto da etapa anterior
  automaticamente, salvo quando `--input-manifest` e informado.

## Modelo de Dados

O snapshot diario gera tres tabelas principais:

- `listings_unificados`: anuncios normalizados por fonte, tipo de negocio e
  identificador do anuncio.
- `properties_unified`: propriedades canonicas deduplicadas.
- `property_listing_link`: relacao entre anuncios e propriedades canonicas.

O schema canonico de listings e definido em `pipelines/normalize.py` por
`CANONICAL_COLUMNS`. Campos textuais longos como `description` e
`long_description` sao usados no processamento, mas removidos da saida final de
listings pelo projetor em `pipelines/daily_snapshot.py`.

O historico usa upsert por:

```text
source | business_type | property_id
```

e adiciona metadados de historico:

```text
first_seen_at, last_seen_at, created_at, updated_at
```

## Utilitarios

Exportar Parquet para CSV:

```powershell
python parquet_to_csv.py --date DD-MM-YYYY
python parquet_to_csv.py --input processed/listings_unificados.parquet --output processed/listings_unificados.csv
```

Recuperar listings ausentes do QuintoAndar para uma data especifica:

```powershell
python workaround_quinto_missing.py paths --date DD-MM-YYYY
python workaround_quinto_missing.py build-missing --date DD-MM-YYYY
python workaround_quinto_missing.py collect-missing --date DD-MM-YYYY
python workaround_quinto_missing.py merge --date DD-MM-YYYY
```

`pipelines/zipcode_enrichment.py` enriquece CEPs e coordenadas durante a
construcao do snapshot diario. O cache fica junto dos outputs processados.

## Testes

Rodar a bateria principal:

```powershell
python -m pytest tests/test_olx_discovery.py tests/test_scrapy_runner.py tests/test_detail_parsers_and_schema.py tests/test_throttle_and_stage.py
```

Com Anaconda:

```powershell
C:\ProgramData\anaconda3\python.exe -m pytest tests/test_olx_discovery.py tests/test_scrapy_runner.py tests/test_detail_parsers_and_schema.py tests/test_throttle_and_stage.py
```

Rodar tudo:

```powershell
python -m pytest
```

Um warning de cache do pytest em `.pytest_cache` pode aparecer em ambientes com
permissao restrita; isso nao afeta o resultado dos testes.

## Estrutura do Repositorio

```text
cli.py                    CLI principal do pipeline
main_pipeline.py           Atalho para `python cli.py run-all`
parquet_to_csv.py          Exportador Parquet -> CSV
workaround_quinto_missing.py
pipelines/                 Normalizacao, dedupe, snapshot, historico e CEP
scrapers/                  Discovery e listings por fonte
stages/                    Stages orquestrados pelo workflow
workflow/                  Runner, manifests, logging, paths e modelos
tests/                     Testes unitarios e de integracao leve
```

## Versionamento de Dados

Os diretorios abaixo sao ignorados pelo Git:

```text
raw/
processed/
artifacts/
logs/
```

Eles podem conter arquivos grandes e outputs reproduziveis. Se algum dataset
precisar ser preservado fora da maquina local, use Git LFS, DVC, releases ou
storage externo em vez de commitar os arquivos diretamente.

## Troubleshooting

- `manifesto de entrada nao encontrado`: rode a etapa anterior para a mesma data
  ou passe `--input-manifest`.
- Discovery ja existente foi reutilizado: use `--force-discovery`.
- Coleta completa interrompida: rode novamente a mesma etapa/data; o resume em
  `artifacts/<data>/collect_listings` deve evitar repetir itens completos.
- Push para GitHub falha por arquivos grandes: confirme que `raw/`, `processed/`
  e `artifacts/` nao estao rastreados com `git ls-files raw processed artifacts`.
