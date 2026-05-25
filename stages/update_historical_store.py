from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.historical_store import HISTORY_COLUMNS, UPSERT_KEY_COLUMNS, update_historical_store
from workflow.models import ArtifactRecord, StageResult, ValidationResult
from workflow.paths import build_scoped_output_dir, build_source_scope_token, normalize_selected_sources
from workflow.stages import Stage


class UpdateHistoricalStoreStage(Stage):
    name = "update_historical_store"
    objective = "Incorporar o snapshot diario validado na base historica incremental."
    inputs = ["build_daily_snapshot manifest"]
    block_on_failure = True

    def run(self, context, input_manifest, logger, stage_options=None):
        if not input_manifest:
            raise ValueError("manifesto da fase anterior e obrigatorio")
        if input_manifest.get("status") != "success":
            raise ValueError("o manifesto do snapshot diario precisa estar validado com status success")

        artifact_map = {artifact["name"]: artifact for artifact in input_manifest.get("artifacts", [])}
        required = [
            "daily_listings",
            "daily_properties",
            "daily_property_listing_link",
        ]
        missing = [name for name in required if name not in artifact_map]
        if missing:
            raise ValueError(f"artefatos obrigatorios ausentes no manifesto diario: {missing}")

        selected_sources = normalize_selected_sources((stage_options or {}).get("sources"))
        scope_token = build_source_scope_token(selected_sources)
        snapshot_listings = pd.read_parquet(artifact_map["daily_listings"]["path"])
        pd.read_parquet(artifact_map["daily_properties"]["path"])
        pd.read_parquet(artifact_map["daily_property_listing_link"]["path"])
        if selected_sources:
            snapshot_listings = snapshot_listings[snapshot_listings["source"].isin(selected_sources)].copy()
            if snapshot_listings.empty:
                raise ValueError(f"nenhum registro encontrado para fontes solicitadas: {selected_sources}")

        processed_dir = build_scoped_output_dir(context.processed_dir, selected_sources)
        output = update_historical_store(snapshot_listings, processed_dir)
        listings_df = output["listings"]
        properties_df = output["properties"]
        links_df = output["links"]
        scope_metadata = {}
        if selected_sources:
            scope_metadata = {
                "source_scope": scope_token,
                "sources": selected_sources,
            }

        artifacts = [
            ArtifactRecord(
                name="historical_listings_latest",
                path=str(output["paths"]["listings"].resolve()),
                format="parquet",
                rows=len(listings_df),
                metadata=dict(scope_metadata),
            ),
            ArtifactRecord(
                name="historical_properties_latest",
                path=str(output["paths"]["properties"].resolve()),
                format="parquet",
                rows=len(properties_df),
                metadata=dict(scope_metadata),
            ),
            ArtifactRecord(
                name="historical_property_listing_link_latest",
                path=str(output["paths"]["links"].resolve()),
                format="parquet",
                rows=len(links_df),
                metadata=dict(scope_metadata),
            ),
        ]
        metrics = {
            "incoming_snapshot_count": len(snapshot_listings),
            "inserted_count": output["inserted_count"],
            "updated_count": output["updated_count"],
            "selected_sources": selected_sources,
            "source_scope": scope_token,
            "output_dir": str(processed_dir.resolve()),
            "historical_listings_count": len(listings_df),
            "historical_properties_count": len(properties_df),
            "historical_links_count": len(links_df),
        }
        return artifacts, metrics, []

    def validate(self, context, input_manifest, result: StageResult, logger, stage_options=None):
        validations: list[ValidationResult] = []
        artifact_map = {artifact.name: artifact for artifact in result.artifacts}
        listings_artifact = artifact_map.get("historical_listings_latest")

        if not listings_artifact:
            return [
                ValidationResult(
                    name="historical_listings_present",
                    passed=False,
                    message="O artefato historico principal e obrigatorio.",
                )
            ]

        if not Path(listings_artifact.path).exists():
            return [
                ValidationResult(
                    name="historical_listings_exists",
                    passed=False,
                    message=f"Artefato ausente no disco: {listings_artifact.path}",
                )
            ]

        listings_df = pd.read_parquet(listings_artifact.path)
        upsert_keys = listings_df[UPSERT_KEY_COLUMNS].fillna("").astype(str).agg("|".join, axis=1)
        history_columns_present = all(column in listings_df.columns for column in HISTORY_COLUMNS)
        incoming_count = result.metrics.get("incoming_snapshot_count", 0)
        inserted_count = result.metrics.get("inserted_count", 0)
        updated_count = result.metrics.get("updated_count", 0)

        validations.append(
            ValidationResult(
                name="upsert_accounting",
                passed=incoming_count == inserted_count + updated_count,
                message="A soma de inseridos e atualizados deve bater com o snapshot recebido.",
            )
        )
        validations.append(
            ValidationResult(
                name="historical_key_uniqueness",
                passed=upsert_keys.is_unique,
                message="A chave historica source|business_type|property_id deve ser unica.",
            )
        )
        validations.append(
            ValidationResult(
                name="history_columns_present",
                passed=history_columns_present,
                message="As colunas de historico devem existir na base consolidada.",
            )
        )

        for artifact in result.artifacts:
            validations.append(
                ValidationResult(
                    name=f"artifact_exists::{artifact.name}",
                    passed=Path(artifact.path).exists(),
                    message=f"Artefato deve existir: {artifact.path}",
                )
            )
        return validations
