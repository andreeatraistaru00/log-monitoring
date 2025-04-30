import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Tuple

LOG_FILE = Path("logs.log")       # Path to the input log file
REPORT_FILE = Path("report.log")  # Path to the output report file

WARNING_THRESHOLD = 5
ERROR_THRESHOLD = 10

# --- Logger Setup ---
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter("[%(levelname)s] %(message)s")

# Configure file handler to write to report file
file_handler = logging.FileHandler(REPORT_FILE, encoding="utf-8")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

# --- Core Processing Function ---
def process_logs(file_path: Path, log: logging.Logger = logger) -> None:

  # Dictionary to store active jobs: pid -> (start time, description)
  active_jobs: Dict[str, Tuple[datetime, str]] = {}

  # Open and read the log file line by line
  with file_path.open(newline="") as fp:
    reader = csv.reader(fp)

    for row in reader:
      time_str, description, status, pid = [x.strip() for x in row]

      try:
        timestamp = datetime.strptime(time_str, "%H:%M:%S")
      except ValueError:
        log.warning("Invalid timestamp '%s' in row: %r", time_str, row)
        continue # Skip to the next row if timestamp is invalid

      # Handle START event
      if status == "START":
        active_jobs[pid] = (timestamp, description)

      # Handle END event
      elif status == "END":
        if pid in active_jobs:
          start_time, desc = active_jobs.pop(pid)
          duration = (timestamp - start_time).total_seconds() / 60
          
          # Log message based on duration thresholds
          if duration > ERROR_THRESHOLD:
            log.error("Job '%s' (PID %s) took %.2f minutes", desc, pid, duration)
          elif duration > WARNING_THRESHOLD:
            log.warning("Job '%s' (PID %s) took %.2f minutes", desc, pid, duration)

        else:
          # END received without corresponding START
          log.info("END event with no START for PID %s", pid)
      # Handle unknown statuses
      else:
        log.info("Unknown status for job with PID %s", pid)

 
if __name__ == "__main__":
  """
  Entry point: process log file
  """
  process_logs(LOG_FILE)
  
