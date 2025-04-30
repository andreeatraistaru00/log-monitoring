# Log Processing App

This Python application reads a CSV-formatted log file (`logs.log`), tracks job durations between START and END events, and writes a report to `report.log` indicating whether each job completed slowly (warning), or very slowly (error), based on configurable thresholds.

## Log Processing Logic

1. The script reads the `logs.log` file line by line using the `csv.reader` method.
2. A dictionary called `active_jobs` is used to store ongoing jobs based on `PID` (those that have a START event but no END yet).
3. The time between the `START` and `END` event is calculated in minutes. The script checks if the job duration exceeds predefined thresholds (5 and 10 minutes) to log WARNING, or ERROR messages.
4. The output is written to `report.log` to keep the console clean and log all events for further review.

## Testing

Basic tests are written using `pytest`. They:
  - Check correct log categorization (INFO/WARNING/ERROR)
  - Use a temporary directory to prevent interference with actual log files

