from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from workflow.paths import default_run_date


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Exporta parquet para CSV")
    parser.add_argument("--date", help="Usa o snapshot diario da data informada")
    parser.add_argument("--input", help="Caminho explicito do parquet")
    parser.add_argument("--output", help="Caminho explicito do CSV de saida")
    return parser


def resolve_paths(args: argparse.Namespace) -> tuple[Path, Path]:
    if args.input:
        input_path = Path(args.input)
    elif args.date:
        input_path = Path("processed") / args.date / "listings_unificados.parquet"
    else:
        input_path = Path("processed") / "listings_unificados.parquet"

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = input_path.with_suffix(".csv")

    return input_path, output_path


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    input_path, output_path = resolve_paths(args)

    df = pd.read_parquet(input_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"CSV gerado em {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
