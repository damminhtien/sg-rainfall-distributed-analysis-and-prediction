"""Render a looping README illustration using saved evaluation metrics.

Requires Pillow and the ffmpeg CLI. No cloud resources or raw data are needed.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import shutil
import subprocess
import tempfile
from functools import cache
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ROOT = Path(__file__).resolve().parents[1]
WIDTH, HEIGHT = 1280, 720
SCALE = 2
SECONDS, FPS = 8, 24
BG = "#08131F"
PANEL = "#0D1E2C"
LINE = "#203746"
WHITE = "#EDF5F7"
MUTED = "#93ADBC"
TEAL = "#51E5C5"
ORANGE = "#FFB36B"
FONT_DIRS = (
    Path("/System/Library/Fonts/Supplemental"),
    Path("/usr/share/fonts/truetype/dejavu"),
    Path("C:/Windows/Fonts"),
)


@cache
def font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont:
    """Use a system sans-serif font without bundling licensed font files."""
    names = (
        ("Arial Bold.ttf", "DejaVuSans-Bold.ttf", "arialbd.ttf")
        if bold
        else ("Arial.ttf", "DejaVuSans.ttf", "arial.ttf")
    )
    for directory in FONT_DIRS:
        for name in names:
            path = directory / name
            if path.is_file():
                return ImageFont.truetype(str(path), size * SCALE)
    raise FileNotFoundError("Install Arial or DejaVu Sans to render the animation.")


def coords(values: tuple[float, ...]) -> tuple[int, ...]:
    return tuple(round(value * SCALE) for value in values)


def label(
    draw: ImageDraw.ImageDraw,
    xy: tuple[float, float],
    text: str,
    size: int = 18,
    color: str = WHITE,
    bold: bool = False,
) -> None:
    draw.text(coords(xy), text, font=font(size, bold), fill=color)


def line(
    draw: ImageDraw.ImageDraw,
    points: list[tuple[float, float]],
    color: str = LINE,
    width: float = 1,
) -> None:
    draw.line(
        [coords(point) for point in points],
        fill=color,
        width=max(1, round(width * SCALE)),
        joint="curve",
    )


def box(
    draw: ImageDraw.ImageDraw,
    bounds: tuple[float, float, float, float],
    fill: str = PANEL,
    outline: str = LINE,
    radius: int = 12,
) -> None:
    draw.rounded_rectangle(
        coords(bounds), radius=radius * SCALE, fill=fill, outline=outline, width=SCALE
    )


def dot(
    draw: ImageDraw.ImageDraw, xy: tuple[float, float], radius: float, color: str
) -> None:
    x, y = xy
    draw.ellipse(coords((x - radius, y - radius, x + radius, y + radius)), fill=color)


def blend(color: str, amount: float, base: str = PANEL) -> str:
    """Mix an accent with the panel color for restrained glow effects."""
    values = [
        round(
            int(base[i : i + 2], 16) * (1 - amount) + int(color[i : i + 2], 16) * amount
        )
        for i in (1, 3, 5)
    ]
    return "#" + "".join(f"{value:02x}" for value in values)


def evidence() -> dict:
    config = json.loads((ROOT / "config/experiment.json").read_text())
    with (ROOT / "report/tables/final/final_evaluation_metrics.csv").open(
        newline="", encoding="utf-8"
    ) as source:
        rows = {
            row["model"]: row
            for row in csv.DictReader(source)
            if float(row["threshold_mm"]) == 0.5
        }
    final, baseline = rows["final_pava"], rows["climatology"]
    if final["run_id"] != baseline["run_id"] or final["n"] != baseline["n"]:
        raise ValueError("Final and baseline results must use the same run and cohort.")
    spark = config["compute_profile"]["spark"]
    return {
        "run_id": final["run_id"],
        "years": len(config["years"]),
        "period": f"{min(config['years'])}-{max(config['years'])}",
        "workers": config["compute_profile"]["workers"]["count"],
        "executors": spark["executor_instances"],
        "cores": spark["executor_instances"] * spark["executor_cores"],
        "windows": int(final["n"]),
        "pr_auc": float(final["pr_auc_spark_1000_bins"]),
        "baseline": float(baseline["pr_auc_spark_1000_bins"]),
        "brier": float(final["brier"]),
    }


class Animation:
    """Keep static typography crisp while animating only the workflow."""

    def __init__(self, data: dict) -> None:
        self.data = data
        self.base = Image.new("RGB", (WIDTH * SCALE, HEIGHT * SCALE), BG)
        draw = ImageDraw.Draw(self.base)
        label(draw, (42, 32), "SINGAPORE / DISTRIBUTED ANALYTICS", 15, TEAL, True)
        label(draw, (1018, 32), f"{data['period']} / ARCHIVED DATA", 14, MUTED)
        label(draw, (40, 65), "Rainfall.", 76, WHITE, True)
        label(draw, (360, 65), "At scale.", 76, TEAL, True)
        label(
            draw,
            (44, 160),
            "From five-minute observations to 30-minute rainfall probabilities.",
            23,
            MUTED,
        )
        box(draw, (40, 218, 1240, 530), radius=20)
        for x in (440, 850):
            line(draw, [(x, 242), (x, 508)])
        for x, stage, title, color in (
            (66, "01 / OBSERVE", "Rainfall across the city", TEAL),
            (466, "02 / DISTRIBUTE", "Apache Spark", ORANGE),
            (876, "03 / PREDICT", "The next 30 minutes", TEAL),
        ):
            label(draw, (x, 239), stage, 14, color, True)
            label(draw, (x, 267), title, 25, WHITE, True)
        label(draw, (66, 484), "NEA observations / schematic network", 15, MUTED)
        label(
            draw,
            (466, 484),
            f"{data['executors']} executors / {data['cores']} worker cores",
            15,
            MUTED,
        )
        label(draw, (876, 308), "2024 PR-AUC / threshold: 0.5 mm", 15, MUTED)
        for y, name, value, color in (
            (348, "Final GBT-B", data["pr_auc"], TEAL),
            (411, "Climatology", data["baseline"], MUTED),
        ):
            label(draw, (876, y - 1), name, 17, WHITE)
            label(draw, (1150, y - 1), f"{value:.4f}", 17, color, True)
            box(draw, (876, y + 27, 1212, y + 42), "#172D3C", "#172D3C", 7)
            box(draw, (876, y + 27, 876 + 336 * value, y + 42), color, color, 7)
        label(draw, (876, 484), "GBT-B / isotonic calibration / PAVA", 15, MUTED)
        stats = (
            (str(data["years"]), "YEARS OF INPUT DATA"),
            (str(data["workers"]), "SPARK WORKERS"),
            (f"{data['windows'] / 1e6:.2f}M", "HELD-OUT BASE WINDOWS"),
            (f"{data['brier']:.5f}", "HELD-OUT BRIER SCORE"),
        )
        for index, (value, caption) in enumerate(stats):
            x = 40 + index * 305
            box(draw, (x, 550, x + 285, 650), radius=14)
            label(draw, (x + 20, 561), value, 38, WHITE, True)
            label(draw, (x + 20, 615), caption, 13, MUTED, True)
        label(
            draw,
            (42, 672),
            "Schematic motion. Saved 2024 scores at 0.5 mm. No live weather feed.",
            15,
            MUTED,
        )
        label(draw, (42, 696), f"Evaluation run: {data['run_id']}", 11, MUTED)
        label(draw, (1000, 680), "PySpark / Dataproc / MLlib", 15, TEAL)
        # Deliberately stylized geography and node positions, not station data.
        self.island = [
            (76, 408),
            (89, 380),
            (112, 359),
            (132, 345),
            (149, 326),
            (175, 334),
            (198, 322),
            (218, 339),
            (250, 329),
            (279, 348),
            (306, 353),
            (333, 368),
            (357, 377),
            (381, 399),
            (410, 411),
            (395, 428),
            (366, 430),
            (346, 444),
            (312, 440),
            (290, 453),
            (269, 446),
            (247, 455),
            (222, 439),
            (188, 443),
            (153, 432),
            (127, 437),
            (102, 421),
        ]
        mask = Image.new("L", self.base.size)
        ImageDraw.Draw(mask).polygon([coords(p) for p in self.island], fill=255)
        self.nodes = [
            (x, y)
            for y in range(345, 440, 27)
            for x in range(106, 386, 37)
            if mask.getpixel(coords((x, y)))
        ]
        self.workers = [(x, y) for y in (354, 420) for x in (568, 628, 688, 748)]

    def frame(self, phase: float) -> Image.Image:
        image = self.base.copy()
        draw = ImageDraw.Draw(image)
        draw.polygon([coords(p) for p in self.island], fill="#122F3A")
        line(draw, self.island + [self.island[0]], "#2F7B7E", 1.5)
        # Periodic rain and node pulses make the eight-second loop seamless.
        for index in range(27):
            x = 83 + (index * 47) % 322
            y = 312 + ((index * 23 + phase * 308) % 154)
            strength = math.sin(math.pi * (y - 312) / 154) * 0.50
            line(draw, [(x + 4, y), (x, y + 12)], blend(TEAL, strength), 1)
        for dx, dy, radius in ((190, 459, 5), (211, 467, 4), (234, 470, 3)):
            dot(draw, (dx, dy), radius, "#23505A")
        for index, (x, y) in enumerate(self.nodes):
            pulse = (math.sin(math.tau * (phase * 2 - index / 11)) + 1) / 2
            dot(draw, (x, y), 7 + pulse * 4, blend(TEAL, pulse * 0.14))
            dot(draw, (x, y), 2.4, blend(TEAL, 0.45 + pulse * 0.55))
        for index in (2, 9, 15):
            x, y = self.nodes[index]
            ring = (phase * 2 + index / 7) % 1
            draw.ellipse(
                coords((x - 22 * ring, y - 22 * ring, x + 22 * ring, y + 22 * ring)),
                outline=blend(TEAL, (1 - ring) * 0.6),
                width=SCALE,
            )
        # Eight worker nodes match the saved compute configuration.
        driver, sink = (494, 387), (813, 387)
        for index, worker in enumerate(self.workers):
            path = [driver, worker, sink]
            line(draw, path, "#314755", 1)
            for offset in (0, 0.5):
                progress = (phase * 4 + index / 8 + offset) % 1
                segment = min(int(progress * 2), 1)
                local = (progress * 2) % 1
                a, b = path[segment], path[segment + 1]
                point = (a[0] + (b[0] - a[0]) * local, a[1] + (b[1] - a[1]) * local)
                dot(draw, point, 2.4, ORANGE)
        for index, (x, y) in enumerate(self.workers):
            pulse = (math.sin(math.tau * (phase * 4 - index / 8)) + 1) / 2
            box(
                draw,
                (x - 22, y - 21, x + 22, y + 21),
                "#17303C",
                blend(ORANGE, 0.25 + pulse * 0.55),
                8,
            )
            label(draw, (x - 9, y - 12), f"{index + 1:02d}", 15, ORANGE, True)
        for point in (driver, sink):
            dot(draw, point, 17, "#193440")
            dot(draw, point, 5, ORANGE)
        label(draw, (469, 449), "driver", 13, MUTED)
        label(draw, (787, 449), "merge", 13, MUTED)
        # A narrow traveling highlight keeps measured bars at their true lengths.
        for y, value, color in (
            (382.5, self.data["pr_auc"], TEAL),
            (445.5, self.data["baseline"], MUTED),
        ):
            x = 880 + (336 * value - 8) * ((phase * 2) % 1)
            dot(draw, (x, y), 3, blend("#FFFFFF", 0.6, color))
        for start, end, color in ((416, 466, TEAL), (831, 866, ORANGE)):
            line(draw, [(start, 387), (end, 387)], LINE, 1)
            progress = (phase * 4) % 1
            dot(draw, (start + (end - start) * progress, 387), 2.5, color)
        return image.resize((WIDTH, HEIGHT), Image.Resampling.LANCZOS)


def render(output: Path) -> None:
    if not shutil.which("ffmpeg"):
        raise FileNotFoundError("Install ffmpeg before rendering GIF and MP4 files.")
    data = evidence()
    if data["workers"] != 8 or data["executors"] != 8:
        raise ValueError(
            "Update the eight-node illustration for the new compute profile."
        )
    animation = Animation(data)
    output.mkdir(parents=True, exist_ok=True)
    animation.frame(0.28).save(output / "rainfall-pipeline-poster.png")
    with tempfile.TemporaryDirectory(prefix="rainfall-animation-") as temporary:
        frames = Path(temporary)
        for index in range(SECONDS * FPS):
            animation.frame(index / (SECONDS * FPS)).save(frames / f"{index:04d}.png")
        source = [
            "ffmpeg",
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-framerate",
            str(FPS),
            "-i",
            str(frames / "%04d.png"),
        ]
        subprocess.run(
            source
            + [
                "-c:v",
                "libx264",
                "-crf",
                "18",
                "-preset",
                "slow",
                "-pix_fmt",
                "yuv420p",
                "-movflags",
                "+faststart",
                str(output / "rainfall-pipeline.mp4"),
            ],
            check=True,
        )
        subprocess.run(
            source
            + [
                "-filter_complex",
                (
                    "fps=12,split[a][b];[a]palettegen=max_colors=128:stats_mode=diff[p];"
                    "[b][p]paletteuse=dither=bayer:bayer_scale=4:diff_mode=rectangle"
                ),
                "-loop",
                "0",
                str(output / "rainfall-pipeline.gif"),
            ],
            check=True,
        )
    print(f"Rendered {SECONDS}s loop at {WIDTH}x{HEIGHT}; source run {data['run_id']}.")
    for name in (
        "rainfall-pipeline.gif",
        "rainfall-pipeline.mp4",
        "rainfall-pipeline-poster.png",
    ):
        path = output / name
        print(
            f"{path.relative_to(ROOT) if path.is_relative_to(ROOT) else path}: "
            f"{path.stat().st_size / 1024:.0f} KiB"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=ROOT / "docs/media")
    render(parser.parse_args().output_dir.resolve())
