from __future__ import annotations

import logging
from pathlib import Path

from workflow.models import PipelineContext
from workflow.paths import build_source_scope_token


class LoggerWriter:
    def __init__(self, logger: logging.Logger, level: int) -> None:
        self.logger = logger
        self.level = level
        self._buffer = ""

    def write(self, message: str) -> int:
        if not message:
            return 0

        self._buffer += message
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            line = line.strip()
            if line:
                self.logger.log(self.level, line)
        return len(message)

    def flush(self) -> None:
        remaining = self._buffer.strip()
        if remaining:
            self.logger.log(self.level, remaining)
        self._buffer = ""


def build_stage_logger(
    context: PipelineContext,
    stage_name: str,
    sources: list[str] | tuple[str, ...] | None = None,
) -> tuple[logging.Logger, Path]:
    scope_token = build_source_scope_token(sources)
    log_filename = f"{stage_name}.log" if not scope_token else f"{stage_name}__sources__{scope_token}.log"
    log_path = context.logs_run_dir / log_filename
    log_path.parent.mkdir(parents=True, exist_ok=True)

    logger_name = f"pipeline.{context.run_date}.{stage_name}"
    if scope_token:
        logger_name = f"{logger_name}.sources.{scope_token}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger, log_path

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger, log_path
