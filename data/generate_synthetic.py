#!/usr/bin/env python3
"""
Generate a tiny synthetic Criteo-like dataset for immediate local testing and development.
No need to download the real Kaggle file for initial development and unit testing.

Generates CSV with same schema:
label,I1..I13,C1..C26

Usage:
  python data/generate_synthetic.py --output data/sample_train.csv --rows 5000

This produces realistic-ish random data for CTR ~0.03-0.1, missing values, etc.
Use this for local dev, then replace with real sampled data for final runs/benchmarks.
"""
import argparse
import numpy as np
import pandas as pd
from pathlib import Path


def generate_synthetic(rows: int, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)

    # label: click or not, realistic low CTR ~ 3-8%
    ctr = 0.05
    labels = rng.binomial(1, ctr, size=rows).astype(int)

    # Integer features (I1-I13): some positive counts, many zeros/missing
    # Simulate with poisson + occasional NaN
    i_cols = {}
    for i in range(1, 14):
        vals = rng.poisson(lam=rng.uniform(1, 8), size=rows).astype(float)
        # Introduce missing ~15-30%
        mask = rng.random(rows) < rng.uniform(0.15, 0.30)
        vals[mask] = np.nan
        i_cols[f"I{i}"] = vals

    # Categorical (hashed strings) - simulate with random hex-like.
    # Give the first few columns realistic LOW cardinality (like device type,
    # region, etc.) so full-history top-N-by-CTR aggregates are meaningful even
    # on small local samples; the rest stay high-cardinality like real Criteo.
    c_cols = {}
    low_card = {1: 20, 2: 30, 3: 40}
    for i in range(1, 27):
        unique = int(low_card.get(i, rng.integers(50, 2000)))
        vals = [f"{rng.integers(0, unique):x}" for _ in range(rows)]
        mask = rng.random(rows) < 0.25
        for j in np.where(mask)[0]:
            vals[j] = ""
        c_cols[f"C{i}"] = vals

    df = pd.DataFrame({
        "label": labels,
        **i_cols,
        **c_cols
    })
    return df


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default="data/sample_train.csv")
    parser.add_argument("--rows", type=int, default=5000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print(f"Generating {args.rows} synthetic rows...")
    df = generate_synthetic(args.rows, args.seed)

    df.to_csv(output_path, index=False)
    print(f"Wrote synthetic sample to {output_path}")

    print("\nSample stats:")
    print(f"  Rows: {len(df)}")
    print(f"  CTR: {df['label'].mean():.4f}")
    print(f"  Sample row:\n{df.iloc[0].to_dict()}")


if __name__ == "__main__":
    main()
