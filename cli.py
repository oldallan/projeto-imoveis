from __future__ import annotations

import argparse
import json
import sys

from scrapers.registry import get_scraper_definitions
from workflow import PipelineRunner
from workflow.paths import default_run_date


STAGE_CHOICES = [
    "collect_discovery",
    "collect_listings",
    "build_daily_snapshot",
    "update_historical_store",
]
SOURCE_CHOICES = sorted({definition.source for definition in get_scraper_definitions()})


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pipeline de dados imobiliarios")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_all_parser = subparsers.add_parser("run-all", help="Executa todas as etapas em sequencia")
    run_all_parser.add_argument("--date", default=default_run_date(), help="Data de execucao no formato DD-MM-YYYY")
    run_all_parser.add_argument("--output-path", required=True, help="Raiz persistente para raw, processed, artifacts e logs")
    run_all_parser.add_argument("--from-stage", choices=STAGE_CHOICES)
    run_all_parser.add_argument("--verbose", action="store_true", help="Ativa logging detalhado nos scrapers")
    run_all_parser.add_argument(
        "--force-discovery",
        action="store_true",
        help="Executa discovery mesmo quando ja existe manifesto de sucesso para a data",
    )

    run_stage_parser = subparsers.add_parser("run-stage", help="Executa uma etapa isolada")
    run_stage_parser.add_argument("stage_name", choices=STAGE_CHOICES)
    run_stage_parser.add_argument("--date", default=default_run_date(), help="Data de execucao no formato DD-MM-YYYY")
    run_stage_parser.add_argument("--output-path", required=True, help="Raiz persistente para raw, processed, artifacts e logs")
    run_stage_parser.add_argument("--input-manifest", help="Manifesto da etapa anterior")
    run_stage_parser.add_argument("--verbose", action="store_true", help="Ativa logging detalhado nos scrapers")
    run_stage_parser.add_argument(
        "--sources",
        nargs="+",
        choices=SOURCE_CHOICES,
        help="Limita a execucao a uma ou mais plataformas",
    )

    subparsers.add_parser("list-stages", help="Lista as etapas disponiveis")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    runner = PipelineRunner()

    try:
        if args.command == "list-stages":
            for stage_name in runner.list_stages():
                print(stage_name)
            return 0

        if args.command == "run-stage":
            result = runner.run_stage(
                stage_name=args.stage_name,
                run_date=args.date,
                output_root=args.output_path,
                input_manifest=args.input_manifest,
                verbose=args.verbose,
                sources=args.sources,
            )
            print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False))
            return 0 if result.status == "success" else 1

        if args.command == "run-all":
            result = runner.run_all(
                run_date=args.date,
                output_root=args.output_path,
                from_stage=args.from_stage,
                verbose=args.verbose,
                force_discovery=args.force_discovery,
            )
            print(json.dumps(result, indent=2, ensure_ascii=False))
            return 0 if result["status"] == "success" else 1
    except Exception as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
