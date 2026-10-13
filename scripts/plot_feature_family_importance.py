"""Render the report's final GBT feature-family importance figure."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt
import scienceplots  # noqa: F401  # Registers the SciencePlots styles.

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "report/tables/full-20261012/feature_family_importance.csv"
OUTPUT = ROOT / "report/figures/full-20261012/task2/feature_family_importance.png"


def main() -> None:
    with SOURCE.open(newline="", encoding="utf-8") as source:
        rows = list(csv.DictReader(source))

    rows.sort(key=lambda row: float(row["normalized_importance"]), reverse=True)
    values = [100 * float(row["normalized_importance"]) for row in rows]
    if abs(sum(values) - 100) > 0.01:
        raise ValueError("Feature-family importance values must sum to 100%.")

    labels = [row["feature_family"] for row in rows]
    color = "#39748A"
    plt.style.use(["science", "no-latex"])

    fig, ax = plt.subplots(figsize=(6.4, 4.35), layout="constrained")
    positions = range(len(rows))
    ax.barh(positions, values, height=0.68, color=color, edgecolor="none")
    ax.set_yticks(list(positions), labels)
    ax.invert_yaxis()
    ax.set_xlim(0, 51.5)
    ax.set_xticks(range(0, 51, 10), [f"{tick}%" for tick in range(0, 51, 10)])
    ax.set_xlabel("Normalized split importance")
    ax.set_axisbelow(True)
    ax.xaxis.grid(True, color="#C9D3D8", linewidth=0.55)
    ax.yaxis.grid(False)
    ax.minorticks_off()

    for position, value in enumerate(values):
        ax.text(
            max(value, 0) + 0.45,
            position,
            f"{value:.2f}%",
            va="center",
            ha="left",
            fontsize=8,
            color="#263640",
            clip_on=False,
        )

    ax.tick_params(axis="y", length=0, pad=5, top=False, right=False)
    ax.tick_params(axis="x", direction="out", length=3, top=False, right=False)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(OUTPUT, dpi=300, facecolor="white")
    plt.close(fig)


if __name__ == "__main__":
    main()
