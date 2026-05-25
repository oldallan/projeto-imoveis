from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time as time_module
from dataclasses import dataclass
from datetime import datetime, timedelta

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
DEFAULT_START_WINDOW = "09:00-15:00"
START_WINDOW_PATTERN = re.compile(r"^([01]\d|2[0-3]):([0-5]\d)-([01]\d|2[0-3]):([0-5]\d)$")


@dataclass(frozen=True)
class DailyStartWindow:
    raw: str
    start_minute: int
    end_minute: int


class PipelineArgumentParser(argparse.ArgumentParser):
    def parse_args(
        self,
        args: list[str] | None = None,
        namespace: argparse.Namespace | None = None,
    ) -> argparse.Namespace:
        parsed = super().parse_args(args, namespace)
        if getattr(parsed, "command", None) == "run-all":
            if parsed.daily:
                if parsed.date is not None:
                    self.error("--date nao pode ser usado com --daily")
                if parsed.start_window is None:
                    parsed.start_window = parse_start_window(DEFAULT_START_WINDOW)
            elif parsed.start_window is not None:
                self.error("--start-window requer --daily")
        return parsed


def parse_start_window(value: str) -> DailyStartWindow:
    match = START_WINDOW_PATTERN.fullmatch(value)
    if not match:
        raise argparse.ArgumentTypeError("use o formato HH:MM-HH:MM")

    start_hour, start_minute, end_hour, end_minute = [int(part) for part in match.groups()]
    start_total = start_hour * 60 + start_minute
    end_total = end_hour * 60 + end_minute
    if start_total >= end_total:
        raise argparse.ArgumentTypeError("a janela deve terminar depois do horario inicial")
    return DailyStartWindow(raw=value, start_minute=start_total, end_minute=end_total)


def next_daily_start(now: datetime, window: DailyStartWindow) -> datetime:
    next_date = (now + timedelta(days=1)).date()
    minute_of_day = random.randint(window.start_minute, window.end_minute)
    hour, minute = divmod(minute_of_day, 60)
    return datetime.combine(next_date, datetime.min.time()).replace(hour=hour, minute=minute)


def sleep_until(target: datetime) -> None:
    seconds = max(0.0, (target - datetime.now()).total_seconds())
    time_module.sleep(seconds)


def build_parser() -> argparse.ArgumentParser:
    parser = PipelineArgumentParser(description="Pipeline de dados imobiliarios")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_all_parser = subparsers.add_parser("run-all", help="Executa todas as etapas em sequencia")
    run_all_parser.add_argument("--date", help="Data de execucao no formato DD-MM-YYYY")
    run_all_parser.add_argument("--output-path", required=True, help="Raiz persistente para raw, processed, artifacts e logs")
    run_all_parser.add_argument("--from-stage", choices=STAGE_CHOICES)
    run_all_parser.add_argument("--daily", action="store_true", help="Executa imediatamente e repete uma vez por dia")
    run_all_parser.add_argument(
        "--start-window",
        type=parse_start_window,
        metavar="HH:MM-HH:MM",
        help=f"Janela diaria de inicio aleatorio; default com --daily: {DEFAULT_START_WINDOW}",
    )
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


def run_all_once(args: argparse.Namespace, runner: PipelineRunner) -> tuple[dict, int]:
    result = runner.run_all(
        run_date=args.date or default_run_date(),
        output_root=args.output_path,
        from_stage=args.from_stage,
        verbose=args.verbose,
        force_discovery=args.force_discovery,
    )
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return result, 0 if result["status"] == "success" else 1


def run_all_daily(args: argparse.Namespace, runner: PipelineRunner) -> int:
    exit_code = 0
    while True:
        try:
            _, run_exit_code = run_all_once(args, runner)
            exit_code = max(exit_code, run_exit_code)
        except Exception as exc:
            exit_code = 1
            print(f"[ERRO] {exc}", file=sys.stderr)

        next_start = next_daily_start(datetime.now(), args.start_window)
        print(f"[INFO] Proxima execucao diaria agendada para {next_start:%Y-%m-%d %H:%M}")
        sleep_until(next_start)


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
            if args.daily:
                return run_all_daily(args, runner)
            _, exit_code = run_all_once(args, runner)
            return exit_code
    except KeyboardInterrupt:
        print("[INFO] Encerrado pelo usuario.", file=sys.stderr)
        return 130
    except Exception as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
