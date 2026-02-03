"""
File:     tradeqc.py
Author:   Yuxing Gao
Date:     1 Feb 2026
Email:    y.yuxinggao@gmail.com

Trade data quality control pipline with the following steps:
1. clean raw trades
2. aggregate into 1-minute OHLCV bars
3. compute rolling VWAP and median volume
4. flag anomalies
5. generate report
"""

import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timezone

TRADES_HEADER = ["timestamp", "instrument", "price", "volume", "trade_id"]

def parse_args():
    p = argparse.ArgumentParser(description="tradeqc – trade data QC pipeline")
    p.add_argument("--input", default="trades_big.csv",
                    help="Path to raw trades CSV.")
    p.add_argument("--outdir", default="output/",
                    help="Output directory.")
    p.add_argument("--N", type=int, default=5,
                    help="Rolling VWAP window (default: 5).")
    p.add_argument("--M", type=int, default=60,
                    help="Rolling median volume window (default: 60).")
    p.add_argument("--k", type=float, default=10,
                    help="Anomaly threshold multiplier (default: 10).")

    args = p.parse_args()
    os.makedirs(args.outdir, exist_ok=True)
    return args


# 3.1 Data Cleaning & Validation
def parse_timestamp(value):
    """Parse an ISO-8601 UTC timestamp string, or return None on failure."""
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def is_valid_number(value):
    """Return True if value is a non-missing, positive number."""
    try:
        return float(value) > 0
    except (ValueError, TypeError):
        return False


def clean_trades(input_path, output_path):
    """Read raw CSV, drop invalid/duplicate rows, write cleaned CSV.
    Returns cleaning stats: total, kept, skipped, skip_reasons.
    """
    skip_counts = defaultdict(int)
    seen_ids = set()
    total = 0
    kept = 0

    with (
        open(input_path, newline="") as fin,
        open(output_path, "w", newline="") as fout,
    ):
        reader = csv.reader(fin)
        writer = csv.writer(fout)

        header = next(reader)
        if header != TRADES_HEADER:
            print(f"WARNING: unexpected header {header}", file=sys.stderr)
        writer.writerow(header)

        for row in reader:
            total += 1

            if len(row) != len(TRADES_HEADER):
                skip_counts["bad_column_count"] += 1
                continue

            timestamp, instrument, price, volume, trade_id = row

            if not is_valid_number(price):
                skip_counts["invalid_price"] += 1
                continue

            if not is_valid_number(volume):
                skip_counts["invalid_volume"] += 1
                continue

            if parse_timestamp(timestamp) is None:
                skip_counts["invalid_timestamp"] += 1
                continue

            if not instrument.strip():
                skip_counts["missing_instrument"] += 1
                continue

            if not trade_id.strip():
                skip_counts["missing_trade_id"] += 1
                continue

            if trade_id in seen_ids:
                skip_counts["duplicate_trade_id"] += 1
                continue
            seen_ids.add(trade_id)

            writer.writerow(row)
            kept += 1

    skip_reasons = {r: c for r, c in skip_counts.items() if c > 0}
    return {
        "total": total,
        "kept": kept,
        "skipped": sum(skip_counts.values()),
        "skip_reasons": skip_reasons,
    }


# 3.2 1-Minute OHLCV Aggregation
def build_ohlcv_bars(input_path):
    """Bucket clean trades into 1-minute OHLCV bars, per instrument.

    Returns (dict[instrument -> list[bar]], total_trades).
    Minutes without trades are omitted.
    """
    # Group trades by (instrument, minute)
    buckets = defaultdict(list)
    total_trades = 0

    with open(input_path, newline="") as f:
        for row in csv.DictReader(f):
            total_trades += 1
            dt = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
            # Round timestamp down to the minute
            minute_str = dt.replace(
                    second=0, microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")
            price = float(row["price"])
            volume = float(row["volume"])
            buckets[(row["instrument"], minute_str)].append((dt, price, volume))

    # Aggregate each bucket into an OHLCV bar
    instruments = defaultdict(list)
    for (instrument, minute_str), trades in buckets.items():
        # sort by exact timestamp
        trades.sort(key=lambda t: t[0])
        prices = [t[1] for t in trades]
        total_volume = sum(t[2] for t in trades)
        pv_sum = sum(t[1] * t[2] for t in trades)

        instruments[instrument].append({
            "minute_start": minute_str,
            "open": round(prices[0], 4),
            "high": round(max(prices), 4),
            "low": round(min(prices), 4),
            "close": round(prices[-1], 4),
            "volume": round(total_volume, 4),
            "pv_sum": round(pv_sum, 4),
        })

    for inst in instruments:
        instruments[inst].sort(key=lambda b: b["minute_start"])
    return dict(instruments), total_trades


# 3.3 Rolling Indicators & Anomaly Detection
def compute_rolling_vwap(bars, n):
    """Rolling over the last N bars: VWAP = sum(price*volume) / sum(volume)."""
    vwaps = []
    for i in range(len(bars)):
        window = bars[max(0, i - n + 1):i + 1]
        total_vol = sum(b["volume"] for b in window)
        if total_vol == 0:
            vwaps.append(None)
        else:
            total_pv = sum(b["pv_sum"] for b in window)
            vwaps.append(round(total_pv / total_vol, 4))
    return vwaps


def compute_rolling_median_volume(bars, m):
    """Rolling median volume over the last M bars."""
    medians = []
    for i in range(len(bars)):
        window = bars[max(0, i - m + 1):i + 1]
        medians.append(round(statistics.median(b["volume"] for b in window), 4))
    return medians


def detect_anomalies(volumes, medians, k):
    """Flag bars where minute_volume > k * rolling_median_volume."""
    flags = []
    for vol, med in zip(volumes, medians):
        if med is None or med == 0:
            flags.append(False)
        else:
            flags.append(vol > k * med)
    return flags


# 4.1 Per-Instrument CSV Output
def write_instrument_csv(outdir, instrument, bars, vwaps, medians, anomalies, n, m):
    """Write aggregated_instrument.csv."""
    path = os.path.join(outdir, f"aggregated_{instrument}.csv")
    header = [
        "minute_start", "open", "high", "low", "close", "volume",
        f"vwap_rolling{n}", f"medianvol_{m}", "is_anomaly",
    ]
    with open(path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        for bar, vwap, med, anom in zip(bars, vwaps, medians, anomalies):
            writer.writerow([
                bar["minute_start"],
                bar["open"], bar["high"], bar["low"], bar["close"],
                bar["volume"],
                vwap if vwap is not None else "",
                med,
                anom,
            ])
    return path


# 4.2 Text Report
def generate_report(outdir, instrument_stats, overall, cleaning):
    """Write report.txt: cleaning stats, per-instrument detail, overall summary."""
    path = os.path.join(outdir, "report.txt")

    with open(path, "w") as f:
        f.write("=== Data Cleaning ===\n\n")
        f.write(f"  Input file:     {overall['input_file']}\n")
        f.write(f"  Rows read:      {cleaning['total']}\n")
        f.write(f"  Rows kept:      {cleaning['kept']}\n")
        f.write(f"  Rows dropped:   {cleaning['skipped']}\n")
        if cleaning["skip_reasons"]:
            f.write("\n  Skip reasons:\n")
            for reason, count in cleaning["skip_reasons"].items():
                f.write(f"    {reason}: {count}\n")
        f.write("\n")

        f.write("=== Per-Instrument Detail ===\n")
        for s in sorted(instrument_stats, key=lambda x: x["instrument"]):
            f.write(f"\n  {s['instrument']}\n")
            f.write(f"  Bars:         {s['num_bars']}\n")
            f.write(f"  Total volume: {s['total_volume']}\n")
            f.write(f"  Price range:  {s['price_low']} - {s['price_high']}\n")
            f.write(f"  Anomalies:    {s['num_anomalies']}\n")
            # top 3 anomalous minutes (ranked by volume / median volume)
            top = sorted(s["anomalies"], key=lambda a: a[1] / a[2], reverse=True)[:3]
            if top:
                f.write("    Top anomalies:\n")
                for minute, vol, med in top:
                    ratio = round(vol / med, 2)
                    f.write(f"    {minute}  vol={vol}  median={med}  ratio={ratio}\n")
        f.write("\n")

        anom_pct = (overall["total_anomalies"] / overall["total_bars"] * 100
                    if overall["total_bars"] > 0 else 0)
        f.write("=== Overall Summary ===\n\n")
        f.write(f"  Instruments: {overall['num_instruments']}\n")
        f.write(f"  Total bars:  {overall['total_bars']}\n")
        f.write(f"  Time range:  {overall['time_min']} to {overall['time_max']}\n")
        f.write(f"  Anomalies:   {overall['total_anomalies']} / {overall['total_bars']}"
                f" ({anom_pct:.2f}%)\n")
        f.write(f"\n  Parameters:\n")
        f.write(f"    N (VWAP window):    {overall['n']}\n")
        f.write(f"    M (median window):  {overall['m']}\n")
        f.write(f"    k (anomaly factor): {overall['k']}\n")

    return path


def main():
    args = parse_args()

    # 3.1 Data Cleaning & Validation
    clean_path = os.path.join(args.outdir, "trades_clean.csv")
    print(f"Cleaning {args.input} ...")
    cleaning = clean_trades(args.input, clean_path)
    print(f"  {cleaning['kept']}/{cleaning['total']} rows kept, "
          f"{cleaning['skipped']} dropped.")
    for reason, count in cleaning["skip_reasons"].items():
        print(f"    {reason}: {count}")

    # 3.2 1-Minute OHLCV Aggregation
    print("Building OHLCV bars ...")
    instruments, total_trades = build_ohlcv_bars(clean_path)
    print(f"  {len(instruments)} instruments, {total_trades} trades.")

    # 3.3 Rolling Indicators & Anomaly Detection
    all_stats = []
    for instrument in sorted(instruments):
        bars = instruments[instrument]
        vwaps = compute_rolling_vwap(bars, args.N)
        medians = compute_rolling_median_volume(bars, args.M)
        volumes = [b["volume"] for b in bars]
        anomalies = detect_anomalies(volumes, medians, args.k)

        path = write_instrument_csv(
            args.outdir, instrument, bars, vwaps, medians, anomalies,
            args.N, args.M,
        )
        print(f"  Wrote {path} ({len(bars)} bars)")

        # 4.1 Collect per-Instrument data
        anomaly_details = []
        for bar, med, is_anom in zip(bars, medians, anomalies):
            if is_anom:
                anomaly_details.append((bar["minute_start"], bar["volume"], med))

        all_stats.append({
            "instrument": instrument,
            "num_bars": len(bars),
            "total_volume": sum(b["volume"] for b in bars),
            "price_low": min(b["low"] for b in bars),
            "price_high": max(b["high"] for b in bars),
            "num_anomalies": len(anomaly_details),
            "anomalies": anomaly_details,
        })

    # 4.2 Text Report
    all_minutes = [b["minute_start"] for bl in instruments.values() for b in bl]
    overall = {
        "input_file": args.input,
        "total_trades": total_trades,
        "total_bars": sum(s["num_bars"] for s in all_stats),
        "num_instruments": len(instruments),
        "time_min": min(all_minutes) if all_minutes else "N/A",
        "time_max": max(all_minutes) if all_minutes else "N/A",
        "total_anomalies": sum(s["num_anomalies"] for s in all_stats),
        "n": args.N,
        "m": args.M,
        "k": args.k,
    }
    report_path = generate_report(args.outdir, all_stats, overall, cleaning)
    print(f"  Wrote {report_path}")


if __name__ == "__main__":
    main()
