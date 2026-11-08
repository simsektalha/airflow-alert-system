import logging


def configure_logging(level: int = logging.INFO) -> logging.Logger:
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return logging.getLogger("airflow-failure-responder")


def _mask(s: str, keep: int = 2) -> str:
    if not s:
        return ""
    return s[:keep] + "" if len(s) > keep else ""
