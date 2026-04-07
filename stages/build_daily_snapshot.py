from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.daily_snapshot import LISTINGS_EXCLUDED_OUTPUT_COLUMNS, build_daily_snapshot
from pipelines.normalize import CANONICAL_COLUMNS
from workflow.models import ArtifactRecord, StageResult, ValidationResult
from workflow.stages import Stage


class BuildDailySnapshotStage(Stage):
    name = "build_daily_snapshot"
    objective = "Transformar a coleta bruta do dia em um snapshot diario consolidado."
    inputs = ["collect_general_listings manifest"]
    block_on_failure = True

    def run(self, context, input_manifest, logger):
        if not input_manifest:
            raise ValueError("manifesto da fase anterior e obrigatorio")
        if input_manifest.get("status") != "success":
            raise ValueError("o manifesto de coleta precisa estar validado com status success")

        raw_files = [artifact["path"] for artifact in input_manifest.get("artifacts", []) if artifact.get("format") == "csv"]
        if not raw_files:
            raise ValueError("nenhum arquivo CSV encontrado no manifesto de coleta")

        snapshot = build_daily_snapshot(raw_files, context.processed_run_dir)
        listings_df = snapshot["listings"]
        properties_df = snapshot["properties"]
        links_df = snapshot["links"]
        paths = snapshot["paths"]

        artifacts = [
            ArtifactRecord(
                name="daily_listings",
                path=str(paths["listings"].resolve()),
                format="parquet",
                rows=len(listings_df),
            ),
            ArtifactRecord(
                name="daily_properties",
                path=str(paths["properties"].resolve()),
                format="parquet",
                rows=len(properties_df),
            ),
            ArtifactRecord(
                name="daily_property_listing_link",
                path=str(paths["links"].resolve()),
                format="parquet",
                rows=len(links_df),
            ),
            ArtifactRecord(
                name="daily_listings_csv",
                path=str(paths["listings_csv"].resolve()),
                format="csv",
                required=False,
                rows=len(listings_df),
            ),
        ]

        metrics = {
            "input_files": raw_files,
            "input_file_count": len(raw_files),
            "daily_listings_count": len(listings_df),
            "daily_properties_count": len(properties_df),
            "daily_links_count": len(links_df),
        }
        return artifacts, metrics, []

    def validate(self, context, input_manifest, result: StageResult, logger):
        validations: list[ValidationResult] = []
        artifact_map = {artifact.name: artifact for artifact in result.artifacts}
        listings_artifact = artifact_map.get("daily_listings")
        properties_artifact = artifact_map.get("daily_properties")
        links_artifact = artifact_map.get("daily_property_listing_link")

        if not listings_artifact or not properties_artifact or not links_artifact:
            return [
                ValidationResult(
                    name="required_artifacts_present",
                    passed=False,
                    message="Os artefatos canonicos do snapshot diario sao obrigatorios.",
                )
            ]

        missing_paths = [
            artifact.path
            for artifact in (listings_artifact, properties_artifact, links_artifact)
            if not Path(artifact.path).exists()
        ]
        if missing_paths:
            return [
                ValidationResult(
                    name="required_artifact_paths_exist",
                    passed=False,
                    message=f"Artefatos ausentes no disco: {missing_paths}",
                )
            ]

        listings_df = pd.read_parquet(listings_artifact.path)
        properties_df = pd.read_parquet(properties_artifact.path)
        links_df = pd.read_parquet(links_artifact.path)
        expected_listing_columns = [
            column for column in CANONICAL_COLUMNS if column not in LISTINGS_EXCLUDED_OUTPUT_COLUMNS
        ]

        validations.append(
            ValidationResult(
                name="canonical_schema_present",
                passed=all(column in listings_df.columns for column in expected_listing_columns),
                message="O schema canonico deve estar presente no snapshot diario.",
            )
        )
        validations.append(
            ValidationResult(
                name="daily_snapshot_non_empty",
                passed=not listings_df.empty,
                message="O snapshot diario nao pode ficar vazio.",
            )
        )

        key_valid = (
            listings_df["source"].notna()
            & listings_df["business_type"].notna()
            & (
                (listings_df["property_id"].fillna("").astype(str).str.len() > 0)
                | (listings_df["listing_url"].fillna("").astype(str).str.len() > 0)
            )
        )
        validations.append(
            ValidationResult(
                name="minimum_business_keys",
                passed=bool(key_valid.all()) if len(key_valid) else False,
                message="Cada listing deve possuir source, business_type e property_id ou listing_url.",
            )
        )

        property_ids = set(properties_df["canonical_property_id"].dropna().astype(str))
        link_ids = set(links_df["canonical_property_id"].dropna().astype(str))
        validations.append(
            ValidationResult(
                name="link_integrity",
                passed=bool(link_ids) and link_ids.issubset(property_ids),
                message="Todos os links devem apontar para propriedades existentes.",
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
