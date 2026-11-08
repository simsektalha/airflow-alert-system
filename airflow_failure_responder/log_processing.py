from typing import Optional
import re
from redaction import redact_text


def crop_log_window(
    log_text: str,
    *,
    # Optional explicit anchors (substring or regex). If not given, we infer smart defaults.
    start_hint: Optional[str] = None,
    end_hint: Optional[str] = None,
    # Keep a little extra context around the window (in lines)
    pre_lines: int = 0,
    post_lines: int = 0,
    # Final safety cap on size; if the slice is longer, keep the tail (closest to error)
    max_chars: int = 40_000,
) -> str:
    """
    Crop a long Airflow task log to the high-signal window.
    Defaults:
      start ~ first 'Using connection to' / 'Spark-Submit cmd' (setup before the run)
      end   ~ first of 'Task exited with return code' / 'Marking task as FAILED' / end of stacktrace
    If anchors are not found, falls back to a window centered around the last error/traceback.
    """
    if not log_text:
        return ""

    lines = log_text.splitlines()

    # Build default heuristics if no explicit hints provided
    start_patterns = []
    end_patterns = []

    if start_hint:
        start_patterns.append(re.escape(start_hint))
    else:
        start_patterns.extend([
            r"Using connection to:",
            r"Spark-Submit cmd:",
            r"Executing <Task\(SparkSubmitOperator\)",
            r"Started process .* to run task",
        ])

    if end_hint:
        end_patterns.append(re.escape(end_hint))
    else:
        end_patterns.extend([
            r"Task exited with return code \d+",
            r"Marking task as FAILED\.",
            r"airflow\.exceptions\.AirflowException: .*",
        ])

    # Find start index
    start_idx = None
    for i, line in enumerate(lines):
        if any(re.search(pat, line) for pat in start_patterns):
            start_idx = max(0, i - pre_lines)
            break

    # Find end index
    end_idx = None
    if start_idx is not None:
        for j in range(start_idx, len(lines)):
            if any(re.search(pat, lines[j]) for pat in end_patterns):
                end_idx = min(len(lines) - 1, j + post_lines)
                break

    # Fallback: center on the last strong error signal if anchors weren't found
    if start_idx is None or end_idx is None:
        # search for the last error marker
        err_pat = re.compile(r"(Traceback|ERROR|CRITICAL|AnalysisException|AirflowException)", re.IGNORECASE)
        err_line = None
        for i in range(len(lines) - 1, -1, -1):
            if err_pat.search(lines[i]):
                err_line = i
                break
        if err_line is None:
            # nothing detected; return tail capped
            cropped = "\n".join(lines[-5000:])  # keep last ~5k lines as a coarse fallback
            return cropped[-max_chars:] if len(cropped) > max_chars else cropped

        # choose a reasonable window around the error
        context_before = 200  # lines
        context_after = 200   # lines
        start_idx = max(0, err_line - context_before)
        end_idx = min(len(lines) - 1, err_line + context_after)

    # Slice and join
    cropped_lines = lines[start_idx:end_idx + 1]
    cropped = "\n".join(cropped_lines)

    # Final size cap (prefer the tail closest to the error)
    if len(cropped) > max_chars:
        cropped = cropped[-max_chars:]

    return cropped


_ERR_LINE_RE = re.compile(r"(Exception|Error|Traceback|CRITICAL)\b", re.IGNORECASE)


def last_error_line(log_text: str, max_len: int = 500) -> str:
    """Return the last error-looking line (used for the fallback card when the LLM fails)."""
    for line in reversed((log_text or "").splitlines()):
        if _ERR_LINE_RE.search(line):
            return line.strip()[:max_len]
    return ""


def process_logs(full_log: str, max_chars: int = 50000) -> str:
    """
    Process full task logs with redaction and intelligent truncation.
    Returns the processed log text ready for AI analysis.
    """
    if not full_log:
        return ""

    # Redact sensitive information
    processed_log = redact_text(full_log)

    # If log is too long, truncate intelligently
    if len(processed_log) > max_chars:
        lines = processed_log.splitlines()

        # Keep the beginning (task setup) and end (failure) of the log
        # This preserves context while staying within limits
        start_lines = lines[:max_chars // 4]  # First quarter
        end_lines = lines[-(max_chars // 2):]  # Last half

        # Add truncation indicator
        truncated_log = "\n".join(start_lines) + "\n\n... [LOG TRUNCATED] ...\n\n" + "\n".join(end_lines)
        processed_log = truncated_log

    return processed_log
