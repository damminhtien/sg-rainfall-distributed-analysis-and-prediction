"""Plot only bounded Spark aggregates; display and export report figures."""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


def collect_small(frame, limit=20000):
    rows = frame.limit(limit + 1).collect()
    if len(rows) > limit:
        raise ValueError("Aggregate exceeds driver collection limit.")
    return pd.DataFrame([row.asDict() for row in rows])


def save_figure(figure, output, name):
    path = Path(output)
    path.mkdir(parents=True, exist_ok=True)
    figure.savefig(path / f"{name}.pdf", bbox_inches="tight")
    figure.savefig(path / f"{name}.png", dpi=180, bbox_inches="tight")
    plt.show()
    plt.close(figure)


def pattern_figures(aggregates, coverage, config, output):
    import scienceplots  # noqa: F401 — Registers the required report styles.

    plt.style.use(["science", "no-latex"])
    plt.rcParams.update(
        {"font.size": 10, "axes.spines.top": False, "axes.spines.right": False}
    )
    annual_coverage = collect_small(coverage)
    matrix = annual_coverage.pivot(
        index="station_id", columns="year", values="coverage"
    )
    fig, ax = plt.subplots(figsize=(8, max(4, len(matrix) * 0.08)))
    image = ax.imshow(matrix.fillna(0), aspect="auto", vmin=0, vmax=1, cmap="viridis")
    ax.set_xticks(range(len(matrix.columns)), matrix.columns)
    ax.set_yticks(
        range(0, len(matrix), max(1, len(matrix) // 20)),
        matrix.index[:: max(1, len(matrix) // 20)],
    )
    ax.set(xlabel="Year", ylabel="Station ID")
    fig.colorbar(image, ax=ax, label="Valid slots / full-calendar slots")
    save_figure(fig, output, "coverage_heatmap")
    monthly = collect_small(aggregates["monthly"])
    hourly = collect_small(aggregates["hourly"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for year, group in monthly.groupby("year") if not monthly.empty else []:
        group = group.sort_values("month")
        axes[0].plot(
            group["month"], group["mean_observed_total_mm"], marker=".", label=str(year)
        )
    axes[0].set(
        xlabel="Month",
        ylabel="Mean observed station-month rainfall (mm)",
        xticks=range(1, 13),
    )
    axes[0].legend(fontsize=8, ncol=2)
    if hourly.empty:
        axes[1].text(
            0.5,
            0.5,
            "No eligible month-hour groups",
            ha="center",
            transform=axes[1].transAxes,
        )
    else:
        hour_matrix = hourly.pivot(
            index="month", columns="hour", values="mean_wet_frequency"
        )
        hour_matrix = hour_matrix.reindex(index=range(1, 13), columns=range(24))
        image = axes[1].imshow(
            hour_matrix,
            aspect="auto",
            origin="lower",
            extent=[-0.5, 23.5, 0.5, 12.5],
            cmap="Blues",
        )
        fig.colorbar(image, ax=axes[1], label="Equal-group mean wet fraction")
    axes[1].set(xlabel="Hour of interval end (SGT)", ylabel="Month")
    save_figure(fig, output, "monthly_hourly_patterns")
    annual = collect_small(aggregates["annual_common_cohort"])
    fig, ax = plt.subplots(figsize=(7, 4))
    for cutoff, group in annual.groupby("coverage_cutoff") if not annual.empty else []:
        group = group.sort_values("year")
        ax.plot(
            group["year"],
            group["mean_observed_total_mm"],
            marker="o",
            label=f"Coverage ≥ {cutoff:.0%}; {int(group.common_stations.iloc[0])} stations",
        )
    if annual.empty:
        ax.text(
            0.5,
            0.5,
            "No common cohort meets the fixed coverage cutoffs",
            transform=ax.transAxes,
            ha="center",
        )
    else:
        ax.legend(fontsize=8)
    ax.set(xlabel="Year", ylabel="Mean observed annual station rainfall (mm)")
    save_figure(fig, output, "annual_patterns")


def evaluation_figures(curves, calibration, output):
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for name, frame in curves.items():
        data = collect_small(frame)
        for threshold, group in data.groupby("threshold_mm"):
            axes[0].plot(
                group.sort_values("recall")["recall"],
                group.sort_values("recall")["precision"],
                label=f"{name}; τ={threshold:g} mm",
            )
    axes[0].set(xlabel="Recall", ylabel="Precision", xlim=(0, 1), ylim=(0, 1))
    axes[0].legend(fontsize=6)
    for name, frame in calibration.items():
        data = collect_small(frame)
        for threshold, group in data.groupby("threshold_mm"):
            group = group.sort_values("predicted_probability")
            axes[1].plot(
                group["predicted_probability"],
                group["observed_frequency"],
                marker=".",
                label=f"{name}; τ={threshold:g} mm",
            )
    axes[1].plot([0, 1], [0, 1], "k--", linewidth=1)
    axes[1].set(
        xlabel="Mean predicted probability",
        ylabel="Observed exceedance frequency",
        xlim=(0, 1),
        ylim=(0, 1),
    )
    axes[1].legend(fontsize=6)
    save_figure(fig, output, "probability_quality")


def eda_figures(aggregates, output):
    """Export distribution/support panels from already materialised aggregates."""
    import scienceplots  # noqa: F401 — Registers styles without requiring a system TeX install.

    plt.style.use(["science", "no-latex"])
    distribution = collect_small(aggregates["distribution_by_year"]).sort_values("year")
    histogram = collect_small(aggregates["rainfall_histogram"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    axes[0].bar(distribution.year.astype(str), distribution.wet_frequency * 100)
    axes[0].set(xlabel="Year", ylabel="Wet valid intervals (%)")
    if not histogram.empty:
        bins = [
            "1: (0, 0.2]",
            "2: (0.2, 0.5]",
            "3: (0.5, 1]",
            "4: (1, 2]",
            "5: (2, 5]",
            "6: (5, 10]",
            "7: (10, 20]",
            "8: (20, 50]",
            "9: >50",
        ]
        for year, group in histogram.groupby("year"):
            wet = group[group.rainfall_bin != "0: dry"].set_index("rainfall_bin")[
                "count"
            ]
            if wet.sum() > 0:
                proportions = wet.reindex(bins, fill_value=0) / wet.sum()
                axes[1].plot(
                    range(len(bins)),
                    proportions.where(proportions > 0),
                    marker=".",
                    label=str(year),
                )
        axes[1].set_xticks(
            range(len(bins)), [b.split(": ")[1] for b in bins], rotation=45, ha="right"
        )
        axes[1].set_yscale("log")
        axes[1].legend(fontsize=7, ncol=2)
    axes[1].set(
        xlabel="Five-minute rainfall bin (mm)",
        ylabel="Fraction among wet readings (log scale)",
    )
    save_figure(fig, output, "rainfall_distribution")

    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for index, label in [(0, "Median"), (1, "P90"), (2, "P95"), (3, "P99")]:
        axes[0].plot(
            distribution.year,
            distribution.wet_quantiles_mm.map(
                lambda x, index=index: x[index] if x else float("nan")
            ),
            marker=".",
            label=label,
        )
    axes[0].set(xlabel="Year", ylabel="Wet five-minute rainfall quantile (mm)")
    axes[0].legend(fontsize=8)
    axes[1].bar(
        distribution.year.astype(str),
        distribution.training_wet_p99_exceedance_frequency * 100,
    )
    reference = distribution.training_wet_p99_mm.iloc[0]
    axes[1].set(
        xlabel="Year",
        ylabel=f"Valid intervals exceeding train wet P99 ({reference:g} mm), %",
    )
    save_figure(fig, output, "rainfall_tail")

    daily = collect_small(aggregates["complete_daily_patterns"])
    completeness = collect_small(aggregates["daily_completeness"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for year, group in daily.groupby("year") if not daily.empty else []:
        group = group.sort_values("month")
        axes[0].plot(
            group.month,
            group.equal_station_mean_complete_day_mm,
            marker=".",
            label=str(year),
        )
    if not daily.empty:
        axes[0].legend(fontsize=7, ncol=2)
    else:
        axes[0].text(
            0.5,
            0.5,
            "No complete station-days",
            transform=axes[0].transAxes,
            ha="center",
        )
    for year, group in completeness.groupby("year"):
        group = group.sort_values("month")
        axes[1].plot(
            group.month,
            group.complete_fraction_of_calendar_days * 100,
            marker=".",
            label=str(year),
        )
    axes[0].set(
        xlabel="Month",
        ylabel="Equal-station mean complete-day rainfall (mm)",
        xticks=range(1, 13),
    )
    axes[1].set(
        xlabel="Month",
        ylabel="Complete / fixed-network calendar station-days (%)",
        xticks=range(1, 13),
        ylim=(0, 100),
    )
    axes[1].legend(fontsize=7, ncol=2)
    save_figure(fig, output, "complete_daily_patterns")

    monthly = collect_small(aggregates["monthly"])
    fig, axes = plt.subplots(1, 3, figsize=(12, 3.6))
    for year, group in monthly.groupby("year") if not monthly.empty else []:
        group = group.sort_values("month")
        axes[0].plot(
            group.month, group.mean_wet_frequency * 100, marker=".", label=str(year)
        )
        axes[1].plot(
            group.month, group.mean_wet_intensity_mm, marker=".", label=str(year)
        )
        axes[2].plot(group.month, group.stations, marker=".", label=str(year))
    for ax, ylabel in zip(
        axes,
        [
            "Mean station wet frequency (%)",
            "Mean station wet intensity (mm / 5 min)",
            "Eligible stations",
        ],
    ):
        ax.set(xlabel="Month", ylabel=ylabel, xticks=range(1, 13))
    if not monthly.empty:
        axes[0].legend(fontsize=6, ncol=2)
    save_figure(fig, output, "monthly_frequency_intensity")

    hourly = collect_small(aggregates["hourly_by_year"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for year, group in hourly.groupby("year") if not hourly.empty else []:
        group = group.sort_values("hour")
        axes[0].plot(
            group.hour, group.equal_station_month_wet_frequency * 100, label=str(year)
        )
        axes[1].plot(group.hour, group.station_month_groups, label=str(year))
    axes[0].set(
        xlabel="Hour of interval end (SGT)",
        ylabel="Equal station-month wet frequency (%)",
        xticks=range(0, 24, 3),
    )
    axes[1].set(
        xlabel="Hour of interval end (SGT)",
        ylabel="Eligible station-month groups",
        xticks=range(0, 24, 3),
    )
    if not hourly.empty:
        axes[0].legend(fontsize=7, ncol=2)
    save_figure(fig, output, "hourly_by_year")

    spatial = collect_small(aggregates["station_map"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    if spatial.empty:
        for ax in axes:
            ax.text(
                0.5,
                0.5,
                "No stable, coverage-eligible station coordinates",
                transform=ax.transAxes,
                ha="center",
                wrap=True,
            )
    else:
        for ax, metric, label in zip(
            axes,
            ["mean_annual_wet_frequency", "mean_annual_extreme_frequency"],
            ["Mean annual wet frequency (%)", "Mean annual train-P99 exceedance (%)"],
        ):
            image = ax.scatter(
                spatial.longitude,
                spatial.latitude,
                c=spatial[metric] * 100,
                s=25,
                cmap="viridis",
            )
            ax.set(
                xlabel="Longitude (degrees)",
                ylabel="Latitude (degrees)",
                title="Station coordinates; no spatial interpolation",
            )
            fig.colorbar(image, ax=ax, label=label)
    save_figure(fig, output, "station_spatial_patterns")

    spells = collect_small(aggregates["wet_spell_summary"])
    fig, axes = plt.subplots(1, 2, figsize=(10, 4))
    for flag, group in (
        spells.groupby("censored_by_gap_or_boundary") if not spells.empty else []
    ):
        group = group.sort_values("year")
        label = "Gap/boundary censored" if flag else "Observed dry boundaries"
        axes[0].plot(
            group.year,
            group.duration_quantiles_minutes.map(lambda x: x[0]),
            marker="o",
            label=label,
        )
        axes[1].plot(group.year, group.spells, marker="o", label=label)
    axes[0].set(
        xlabel="Year of spell start",
        ylabel="Median observed wet-spell duration (minutes)",
    )
    axes[1].set(xlabel="Year of spell start", ylabel="Observed wet spells (count)")
    if not spells.empty:
        axes[0].legend(fontsize=8)
    save_figure(fig, output, "wet_spell_patterns")


def export_eda_tables(aggregates, output):
    """Save the exact plotted aggregates as CSV and a compact evidence summary."""
    import json

    path = Path(output)
    path.mkdir(parents=True, exist_ok=True)
    tables = {}
    for name, frame in aggregates.items():
        table = collect_small(frame)
        keys = [
            key
            for key in [
                "year",
                "month",
                "hour",
                "station_id",
                "coverage_cutoff",
                "rainfall_bin",
                "censored_by_gap_or_boundary",
            ]
            if key in table
        ]
        if keys:
            table = table.sort_values(keys)
        table.to_csv(path / f"{name}.csv", index=False)
        tables[name] = table
    findings = {
        "status": "computed_from_materialised_spark_aggregates",
        "wet_definition": "rain_mm > 0",
        "analysed_years": [int(year) for year in tables["distribution_by_year"].year],
        "training_wet_p99_mm": float(
            tables["distribution_by_year"].training_wet_p99_mm.iloc[0]
        ),
        "annual_cohort_support": tables["annual_cohort_support"].to_dict(
            orient="records"
        ),
        "distribution_weighting": "valid observations; not equal-station weighted",
        "annual_comparison": [],
        "monthly_extrema": [],
        "hourly_extrema": [],
    }
    for year, group in (
        tables["monthly"].groupby("year") if not tables["monthly"].empty else []
    ):
        peak, trough = (
            group.loc[group.mean_observed_total_mm.idxmax()],
            group.loc[group.mean_observed_total_mm.idxmin()],
        )
        findings["monthly_extrema"].append(
            {
                "year": int(year),
                "represented_months": len(group),
                "peak_month": int(peak.month),
                "peak_observed_total_mm": float(peak.mean_observed_total_mm),
                "peak_stations": int(peak.stations),
                "trough_month": int(trough.month),
                "trough_observed_total_mm": float(trough.mean_observed_total_mm),
                "trough_stations": int(trough.stations),
                "station_cohort_can_change_between_months": True,
            }
        )
    for cutoff, group in (
        tables["annual_common_cohort"].groupby("coverage_cutoff")
        if not tables["annual_common_cohort"].empty
        else []
    ):
        peak, trough = (
            group.loc[group.mean_observed_total_mm.idxmax()],
            group.loc[group.mean_observed_total_mm.idxmin()],
        )
        findings["annual_comparison"].append(
            {
                "coverage_cutoff": float(cutoff),
                "common_stations": int(peak.common_stations),
                "highest_year": int(peak.year),
                "lowest_year": int(trough.year),
                "observed_total_difference_mm": float(
                    peak.mean_observed_total_mm - trough.mean_observed_total_mm
                ),
            }
        )
    for year, group in (
        tables["hourly_by_year"].groupby("year")
        if not tables["hourly_by_year"].empty
        else []
    ):
        peak, trough = (
            group.loc[group.equal_station_month_wet_frequency.idxmax()],
            group.loc[group.equal_station_month_wet_frequency.idxmin()],
        )
        findings["hourly_extrema"].append(
            {
                "year": int(year),
                "represented_hours": len(group),
                "peak_hour_sgt": int(peak.hour),
                "trough_hour_sgt": int(trough.hour),
                "wet_frequency_difference_percentage_points": float(
                    100
                    * (
                        peak.equal_station_month_wet_frequency
                        - trough.equal_station_month_wet_frequency
                    )
                ),
                "peak_station_month_groups": int(peak.station_month_groups),
                "trough_station_month_groups": int(trough.station_month_groups),
            }
        )
    findings["common_cohort_available"] = bool(findings["annual_comparison"])
    (path / "findings.json").write_text(
        json.dumps(findings, indent=2, allow_nan=False) + "\n"
    )
    return findings
