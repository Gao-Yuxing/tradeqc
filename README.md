# tradeqc

Quality control pipeline for trade data, including data cleaning,
1-minute OHLCV aggregation, rolling VWAP, and volume anomaly detection.

## Project files

`tradeqc.py`:              single-file CLI tool (main code)

`data_generate.py`:        generate clean test data (1M rows)

`README.md`:               this file

## Usage

### Generate test data:

    python data_generate.py

### Run the pipeline:

    python tradeqc.py --input trades_big.csv --outdir output/

### Arguments

`--input`:  Path to raw trades CSV file

`--outdir`: Output directory

`--N` (optional, default = 5 ): Rolling VWAP window size

`--M` (optional, default = 60): Rolling median volume window size

`--k` (optional, default = 10): Anomaly threshold multiplier

### Example with custom parameters:

    python tradeqc.py --input trades_big.csv --outdir output/ --N 10 --M 120 --k 5

## Pipeline

The tool runs four stages in sequence:

1. Data cleaning
2. OHLCV aggregation
3. Rolling VWAP
4. Anomaly detection

## Output files

All output is written to `--outdir`:

<outdir>/
  trades_clean.csv
  aggregated_TCBT.csv
  aggregated_TGBT.csv
  ...
  report.txt

### Per-instrument CSV columns

`minute_start`:     start of the 1-minute bar (ISO-8601, UTC)

`open`:             opening price

`high`:             highest price

`low`:              lowest price

`close`:            closing price

`volume`:           total volume

`vwap_rolling<N>`:  rolling VWAP over last N bars

`medianvol_<M>`:    rolling median volume over last M bars

`is_anomaly`:       True if volume > k * median, else False

### report.txt

Report contains three sections:

1. Data cleaning detail
2. Per-instrument detail
3. Overall summary


## Solution & design choices

The pipeline is a single Python file with no external dependencies, only use the
standard library.

1. File reads the input row-by-row and writes valid rows immediately, so memory
use stays constant regardless of file size.

2. OHLCV bars are plain dicts grouped by (instrument, minute) in a defaultdict.

3. A bar is flagged when its volume exceeds k times the rolling median. Median
is more robust to outliers than mean.

4. Each stage (clean, aggregate, VWAP, anomaly, report) is its own function with
plain inputs/outputs. This makes each piece independently testable and the main
function a short, readable sequence of calls.
