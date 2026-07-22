#!/usr/bin/env python3
"""
Prepare a manageable sample from the Criteo Display Ad Challenge train.txt (TSV).

Usage:
  python data/prepare_sample.py \
      --input /path/to/train.txt \
      --output data/sample_train.csv \
      --rows 100000 \
      --seed 42

Output is a CSV with header:
label,I1,I2,...,I13,C1,C2,...,C26

Notes:
- Original is tab-separated, first column = label (0/1).
- Missing values are empty or '-'; we keep them as empty for Spark/pandas flexibility.
- Sampling is deterministic via head or pandas sample (first N or random).
- You can also use `head -n 100001 train.txt > sample.txt` then convert.
"""
import argparse
import pandas as pd
import sys
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Path to original train.txt (TSV)")
    parser.add_argument("--output", required=True, help="Path for output sampled CSV")
    parser.add_argument("--rows", type=int, default=100000, help="Number of rows to keep (excluding header)")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for sampling")
    parser.add_argument("--random", action="store_true", help="Use random sample instead of first N rows")
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if not input_path.exists():
        print(f"ERROR: Input file not found: {input_path}", file=sys.stderr)
        sys.exit(1)

    print(f"Reading first lines to determine columns from {input_path} ...")

    # Criteo columns
    cols = ['label'] + [f'I{i}' for i in range(1, 14)] + [f'C{i}' for i in range(1, 27)]

    # Use pandas with chunks or nrows for memory efficiency on huge file
    try:
        if args.random:
            # For random sample on huge file, do two-pass or reservoir, but for simplicity:
            # Read with nrows limit first then sample. For true random on full, user can use other tools.
            df = pd.read_csv(input_path, sep='\t', header=None, names=cols, nrows=args.rows * 3)
            df = df.sample(n=min(args.rows, len(df)), random_state=args.seed)
        else:
            df = pd.read_csv(input_path, sep='\t', header=None, names=cols, nrows=args.rows)

        print(f"Sampled {len(df)} rows.")

        # Optional light cleaning note (no heavy fill here; let downstream handle)
        # df = df.fillna('')  # or specific for ints vs cats

        df.to_csv(output_path, index=False)
        print(f"Wrote sample to {output_path}")

        # Print basic stats for sanity
        print("\nBasic stats on sample:")
        print(f"  Total rows: {len(df)}")
        print(f"  Click rate (label=1): {df['label'].mean():.4f}")
        print(f"  Non-null per column (top 5):")
        print(df.count().head().to_string())

    except Exception as e:
        print(f"Failed to sample: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
