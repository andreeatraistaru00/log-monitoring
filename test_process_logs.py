import logging
from pathlib import Path
import pytest

import process_logs


def write_log_file(tmp_path: Path, lines):
    """
    Helper: write given lines to a logs.log file in tmp_path and return its Path.
    """
    log_file = tmp_path / "logs.log"
    log_file.write_text("\n".join(lines) + "\n")
    return log_file

def get_test_logger(file_path):
    test_logger = logging.getLogger(f"test_report")
    test_logger.setLevel(logging.INFO)
    handler = logging.FileHandler(file_path, mode="w")
    formatter = logging.Formatter("[%(levelname)s] %(message)s")
    handler.setFormatter(formatter)
    test_logger.addHandler(handler)
    return test_logger

@pytest.fixture(autouse=True)
def change_cwd(tmp_path, monkeypatch):
    """
    Run each test in an isolated tmp_path workspace,
    so  process_logs.LOG_FILE points there.
    """
    monkeypatch.chdir(tmp_path)
    yield


def test_warning_above_threshold(tmp_path, caplog):
    # Duration 6 minutes (greater than WARNING_THRESHOLD=5)
    lines = [
        "00:00:00, JobA, START, 42",
        "00:06:00, JobA, END, 42",
    ]
    log_file = write_log_file(tmp_path, lines)

    caplog.set_level(logging.WARNING)
    process_logs.process_logs(log_file, log=get_test_logger("log_file"))

    # Expect a WARNING message about 6.00 minutes
    assert any(
        "Job 'JobA' (PID 42) took 6.00 minutes" in rec.getMessage()
        and rec.levelno == logging.WARNING
        for rec in caplog.records
    )


def test_error_above_error_threshold(tmp_path, caplog):
    # Duration 11 minutes (greater than ERROR_THRESHOLD=10)
    lines = [
        "00:00:00, JobB, START, 100",
        "00:11:00, JobB, END, 100",
    ]
    log_file = write_log_file(tmp_path, lines)

    caplog.set_level(logging.ERROR)
    process_logs.process_logs(log_file, log=get_test_logger("log_file"))

    # Expect an ERROR message about 11.00 minutes
    assert any(
        "Job 'JobB' (PID 100) took 11.00 minutes" in rec.getMessage()
        and rec.levelno == logging.ERROR
        for rec in caplog.records
    )


def test_end_without_start(tmp_path, caplog):
    # END without a prior START should log INFO
    lines = [
        "00:10:00, OrphanTask, END, 999",
    ]
    log_file = write_log_file(tmp_path, lines)

    caplog.set_level(logging.INFO)
    process_logs.process_logs(log_file, log=get_test_logger("log_file"))

    # Expect an INFO about END without matching START
    assert any(
        "END event with no START for PID 999" in rec.getMessage()
        and rec.levelno == logging.INFO
        for rec in caplog.records
    )


def test_unknown_status(tmp_path, caplog):
    # Unknown status should log INFO
    lines = [
        "00:05:00, WeirdTask, UNKNOWN, 555",
    ]
    log_file = write_log_file(tmp_path, lines)

    caplog.set_level(logging.INFO)
    process_logs.process_logs(log_file, log=get_test_logger("log_file"))

    # Expect an INFO about unknown status
    assert any(
        "Unknown status for job with PID 555" in rec.getMessage()
        and rec.levelno == logging.INFO
        for rec in caplog.records
    )


def test_invalid_timestamp_warning(tmp_path, caplog):
    # Invalid timestamp should log a WARNING and not crash
    lines = [
        "notatime, BadJob, START, 123",
        "00:01:00, BadJob, END, 123",
    ]
    log_file = write_log_file(tmp_path, lines)

    caplog.set_level(logging.WARNING)
    process_logs.process_logs(log_file, log=get_test_logger("log_file"))

    # Expect a WARNING about invalid timestamp
    assert any(
        "Invalid timestamp 'notatime' in row" in rec.getMessage()
        and rec.levelno == logging.WARNING
        for rec in caplog.records
    )
